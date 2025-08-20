import asyncio
import os
import re
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from calendar import monthrange
from collections import OrderedDict

import json
import requests

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Font

from playwright.async_api import async_playwright

# Google Drive
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# -------------------------
# 설정
# -------------------------
KST = ZoneInfo("Asia/Seoul")
URL = "https://m.oliveyoung.co.kr/m/mtn?menu=ranking&tab=brands"

OUTPUT_DIR = "data"
XLSX_NAME = "올리브영_브랜드_순위.xlsx"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, XLSX_NAME)

# 환경변수(Secrets)
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID   = os.environ.get("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID   = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "")

# -------------------------
# Playwright (모바일) 크롤링
# -------------------------
async def close_banners(page):
    # 보이는 팝업/앱유도 닫기 시도 (있어도 없어도 통과)
    candidates = [
        "text=닫기", "text=취소", "text=나중에", "role=button[name='닫기']",
        "button[aria-label*='닫기']", "[class*='btn_close']"
    ]
    for sel in candidates:
        try:
            el = await page.query_selector(sel)
            if el:
                await el.click(timeout=1000)
        except Exception:
            pass
    try:
        await page.mouse.wheel(0, 600)
        await page.wait_for_timeout(400)
    except Exception:
        pass

async def scroll_to_bottom(page, pause_ms=700, max_loops=24):
    last_h = 0
    for _ in range(max_loops):
        try:
            h = await page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(pause_ms)
            last_h = h
        except Exception:
            break

async def parse_brands(page):
    """브랜드명 Top100 추출(구조 변화 대비 다중 셀렉터)"""
    texts = []

    # 1) 브랜드 전용 클래스 추정
    preferred = [
        ".brand", ".brandName", ".tx_brand", "span[class*='brand']",
        "strong[class*='brand']", "em[class*='brand']",
    ]
    for sel in preferred:
        nodes = await page.query_selector_all(sel)
        for n in nodes:
            try:
                t = (await n.inner_text()).strip()
                if t:
                    texts.append(t)
            except Exception:
                pass

    # 2) 리스트 텍스트에서 후보 추출(백업)
    if len(texts) < 80:
        list_sels = ["ul li", "ol li", "[class*='rank'] li", "[class*='list'] li"]
        for lsel in list_sels:
            items = await page.query_selector_all(lsel)
            for li in items:
                try:
                    raw = (await li.inner_text()).strip()
                    if not raw:
                        continue
                    lines = [re.sub(r"\s+", " ", s).strip() for s in raw.splitlines()]
                    cand = None
                    for s in lines:
                        if len(s) <= 1:
                            continue
                        s2 = re.sub(r"^[#\d\.\-\)\(]+", "", s).strip()
                        if (
                            s2
                            and not re.match(r"^\d+$", s2)
                            and "랭킹" not in s2
                            and "브랜드" not in s2
                            and "TOP" not in s2.upper()
                        ):
                            cand = s2
                            break
                    if cand:
                        texts.append(cand)
                except Exception:
                    pass

    uniq = list(OrderedDict.fromkeys([t.strip() for t in texts if t.strip()]))
    cleaned = []
    for t in uniq:
        if 2 <= len(t) <= 30 and len(t.split()) <= 6:
            cleaned.append(t)
    return cleaned[:100]

async def scrape_top100():
    async with async_playwright() as p:
        iphone = p.devices.get("iPhone 13 Pro")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(**iphone, locale="ko-KR")
        page = await context.new_page()

        await page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_timeout(1200)
        await close_banners(page)
        await scroll_to_bottom(page)

        brands = await parse_brands(page)

        await context.close()
        await browser.close()
        return brands

# -------------------------
# 엑셀: 월 시트 자동 생성/갱신
# -------------------------
def month_sheet_name(dt: datetime) -> str:
    return f"{dt.strftime('%y')}년 {dt.month}월"  # 예: 25년 9월

def ensure_month_sheet(wb, dt: datetime):
    name = month_sheet_name(dt)
    if name in wb.sheetnames:
        ws = wb[name]
    else:
        ws = wb.create_sheet(title=name)
        setup_layout(ws, dt)
    return ws

def setup_layout(ws, dt: datetime):
    last_day = monthrange(dt.year, dt.month)[1]

    # 타이틀
    ws["A1"] = "브랜드 순위 (올리브영 앱 기준)"
    ws["A1"].font = Font(bold=True, size=12)
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=1 + last_day)

    # 2행: 1~말일
    ws["A2"] = "일자"
    for d in range(1, last_day + 1):
        ws.cell(row=2, column=1 + d).value = f"{d}일"

    # 3행: 요일
    ws["A3"] = "요일"
    for d in range(1, last_day + 1):
        wd = date(dt.year, dt.month, d).weekday()
        ws.cell(row=3, column=1 + d).value = ["월", "화", "수", "목", "금", "토", "일"][wd]

    # 4행: 비고
    ws["A4"] = "비고"

    # 5행~: 순위
    for r in range(1, 101):
        ws.cell(row=4 + r, column=1).value = r

    # 정렬/폭
    for r in range(1, 5 + 100):
        for c in range(1, 1 + last_day + 1):
            ws.cell(row=r, column=c).alignment = Alignment(vertical="center")
    ws.column_dimensions["A"].width = 8
    for d in range(1, last_day + 1):
        ws.column_dimensions[get_column_letter(1 + d)].width = 18

def write_today(ws, now: datetime, brands):
    col = 1 + now.day  # A=1
    for i in range(100):
        ws.cell(row=5 + i, column=col).value = brands[i] if i < len(brands) else None

def read_rank_map(ws, day: int):
    """해당 시트의 day 열을 {브랜드: 순위}로 변환"""
    col = 1 + day
    ranks = {}
    for i in range(100):
        name = ws.cell(row=5 + i, column=col).value
        if name:
            ranks[str(name).strip()] = i + 1
    return ranks

def get_yesterday_rank_map(wb, now: datetime):
    # 같은 달에 전일 데이터가 있으면 사용, 없으면 전월 마지막날을 찾아봄
    y = now - timedelta(days=1)

    # 전일이 같은 달
    sheet_name_today = month_sheet_name(now)
    sheet_name_y = month_sheet_name(y)

    if sheet_name_y in wb.sheetnames:
        ws_y = wb[sheet_name_y]
        # 전일 열이 존재(헤더 작성되어있음)하면 읽기
        try:
            return read_rank_map(ws_y, y.day)
        except Exception:
            pass

    # 없으면 빈 dict
    return {}

def save_excel_and_get_yesterday_map(brands):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    now = datetime.now(KST)

    if os.path.exists(OUTPUT_PATH):
        wb = load_workbook(OUTPUT_PATH)
    else:
        wb = Workbook()
        if "Sheet" in wb.sheetnames and len(wb.sheetnames) == 1:
            wb.remove(wb["Sheet"])

    ws = ensure_month_sheet(wb, now)

    # 레이아웃 보정(혹시 비어있다면)
    if ws.cell(row=2, column=2).value is None:
        setup_layout(ws, now)

    # 전일 랭크 맵 먼저 구함
    ymap = get_yesterday_rank_map(wb, now)

    # 오늘 쓰기
    write_today(ws, now, brands)

    wb.save(OUTPUT_PATH)
    return ymap, now

# -------------------------
# Slack: Top10 알림 (전일 대비 등락 표기)
# 기존 규칙: (↑n)/(↓n)/(-)/(new)
# -------------------------
def build_delta(today_rank, yesterday_rank):
    if yesterday_rank is None:
        return "(new)"
    diff = yesterday_rank - today_rank
    if diff > 0:
        return f"(↑{diff})"
    elif diff < 0:
        return f"(↓{abs(diff)})"
    else:
        return "(-)"

def post_slack_top10(brands, ymap, now):
    if not SLACK_WEBHOOK_URL:
        print("[경고] SLACK_WEBHOOK_URL 미설정 — 슬랙 전송 생략")
        return

    top10 = brands[:10]
    lines = []
    for idx, name in enumerate(top10, start=1):
        y_rank = ymap.get(name)
        delta = build_delta(idx, y_rank)
        lines.append(f"{idx}. {delta} {name}")

    title = f"📊 올리브영 데일리 브랜드 랭킹 Top10 — {now.strftime('%Y-%m-%d')} (KST)"
    body = "\n".join(lines)

    payload = {
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": title, "emoji": True}},
            {"type": "section", "text": {"type": "mrkdwn", "text": body}},
        ]
    }
    try:
        r = requests.post(SLACK_WEBHOOK_URL, data=json.dumps(payload), headers={"Content-Type":"application/json"}, timeout=10)
        r.raise_for_status()
        print("[슬랙] Top10 전송 완료")
    except Exception as e:
        print(f"[슬랙] 전송 실패: {e}")

# -------------------------
# Google Drive 업로드(동일 파일명 존재 시 업데이트)
# -------------------------
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN and GDRIVE_FOLDER_ID):
        print("[경고] 구글 드라이브 시크릿이 없어 업로드를 건너뜁니다.")
        return None

    creds = Credentials(
        token=None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)

def find_file_in_folder(service, folder_id, name):
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, fields="files(id, name)", pageSize=1).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None

def upload_or_update_to_drive(filepath, folder_id):
    service = build_drive_service()
    if not service:
        return

    file_id = find_file_in_folder(service, folder_id, os.path.basename(filepath))
    media = MediaFileUpload(filepath, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", resumable=True)

    if file_id:
        # 업데이트
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"[드라이브] 기존 파일 갱신 완료: {filepath}")
    else:
        file_metadata = {"name": os.path.basename(filepath), "parents": [folder_id]}
        service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        print(f"[드라이브] 새 파일 업로드 완료: {filepath}")

# -------------------------
# main
# -------------------------
async def main():
    brands = await scrape_top100()
    if not brands:
        raise RuntimeError("브랜드명을 찾지 못했습니다. 셀렉터 확인 필요")

    ymap, now = save_excel_and_get_yesterday_map(brands)
    post_slack_top10(brands, ymap, now)
    upload_or_update_to_drive(OUTPUT_PATH, GDRIVE_FOLDER_ID)

if __name__ == "__main__":
    asyncio.run(main())
