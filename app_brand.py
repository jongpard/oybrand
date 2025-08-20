import asyncio
import os
import re
import json
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from calendar import monthrange
from collections import OrderedDict

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
URL = "https://m.oliveyoung.co.kr/m/mtn?menu=ranking&tab=brands&timeSaleDayFilter=today&toggle=OFF"

OUTPUT_DIR = "data"
XLSX_NAME = "올리브영_브랜드_순위.xlsx"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, XLSX_NAME)

# Secrets (환경변수)
SLACK_WEBHOOK_URL    = os.environ.get("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID     = os.environ.get("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "")

# -------------------------
# 금지/정규화 규칙
# -------------------------
BAN_SUBSTRINGS = [
    "이미지","썸네일","로고","타이틀","아이콘","배너","상품","클럽",
    "랭킹","판매","온라인","일간","주간","월간","혜택","쿠폰","무료",
    "무배","배송","오늘드림","오특","증정","기획","세트","이벤트","베스트","BEST"
]
BAN_EXACT = {
    "오늘드림","올리브영","헬스","헬스플러스","럭스에디트","대표전화","고객센터",
    "채팅 상담","사업자 정보 확인","1:문의","전체보기","더보기","더 보기"
}

def normalize_brand_text(t: str) -> str | None:
    if not t:
        return None
    s = re.sub(r"\s+", " ", t).strip()

    # 뒤꼬리 제거 (… 로고/이미지/타이틀/브랜드 썸네일 등)
    s = re.sub(r"\s*(브랜드\s*썸네일|로고.*|이미지.*|타이틀.*)$", "", s).strip()

    if s in BAN_EXACT:
        return None
    if any(x in s for x in BAN_SUBSTRINGS):
        return None
    if len(s) < 1 or len(s) > 30:
        return None
    if len(s.split()) > 6:
        return None
    # 가격/수량 단위 제거
    if re.search(r"(원|%|ml|g)\b", s, re.IGNORECASE):
        return None
    # 지나치게 일반적인 영어 단어
    if s.lower() in {"brand","logo","image","title"}:
        return None
    # 특수문자 과다/순수 숫자 제거
    if re.fullmatch(r"[0-9\W_]+", s):
        return None
    return s

# -------------------------
# Playwright helpers
# -------------------------
async def maybe_click_brand_tab(page):
    """상단 탭에서 '브랜드 랭킹'을 확실히 선택"""
    sels = [
        "role=tab[name='브랜드 랭킹']",
        "button:has-text('브랜드 랭킹')",
        "a:has-text('브랜드 랭킹')",
        "text=브랜드 랭킹",
    ]
    for sel in sels:
        try:
            el = await page.wait_for_selector(sel, timeout=1500)
            if el:
                await el.click(timeout=800)
                await page.wait_for_timeout(500)
                break
        except Exception:
            pass

async def close_banners(page):
    candidates = [
        "[aria-label*='닫기']","button[aria-label*='닫기']","[class*='btn_close']",
        "text=닫기","text=취소","text=나중에",
    ]
    for sel in candidates:
        try:
            el = await page.query_selector(sel)
            if el:
                await el.click(timeout=500)
                await page.wait_for_timeout(150)
        except Exception:
            pass

async def click_more_until_end(page, max_clicks=12):
    texts = ["더보기","더 보기","More","more"]
    for _ in range(max_clicks):
        clicked = False
        for t in texts:
            try:
                btn = await page.query_selector(f"button:has-text('{t}'), a:has-text('{t}')")
                if btn:
                    await btn.click(timeout=800)
                    await page.wait_for_timeout(700)
                    clicked = True
                    break
            except Exception:
                pass
        if not clicked:
            break

async def scroll_to_bottom(page, pause_ms=900, max_loops=60):
    last_h = 0
    for _ in range(max_loops):
        try:
            h = await page.evaluate("document.body.scrollHeight")
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(pause_ms)
            h2 = await page.evaluate("document.body.scrollHeight")
            if h2 == last_h == h:
                break
            last_h = h2
        except Exception:
            break

# -------------------------
# 네트워크(JSON) 기반 추출
# -------------------------
def extract_brands_from_json_objs(json_objs):
    cand = []
    def walk(obj, parent_key=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                kl = str(k).lower()
                if isinstance(v, str):
                    # 키에 brand 포함되면 강하게 후보로
                    if "brand" in kl or "brnd" in kl:
                        nb = normalize_brand_text(v)
                        if nb:
                            cand.append(nb)
                else:
                    walk(v, kl)
        elif isinstance(obj, list):
            for it in obj:
                walk(it, parent_key)

    for jo in json_objs:
        try:
            walk(jo)
        except Exception:
            pass
    uniq = list(OrderedDict.fromkeys(cand))
    # 추가 노이즈 컷
    junk = {"브랜드","랭킹","판매","온라인"}
    return [x for x in uniq if x not in junk][:100]

# -------------------------
# DOM 보조 추출 (브랜드 목록 영역만 스코프)
# -------------------------
BRAND_CONTAINER_CANDIDATES = [
    "[id*='brand'][class*='list']", "[class*='brand'][class*='list']",
    "[class*='brand'][class*='wrap']", "[class*='brand'][class*='container']",
    "#contents, #container, main"
]
BRAND_NAME_CANDIDATE_SELECTORS = [
    ".brand, .brandName, .tx_brand, .brand-name, .tit, .name, .txt, .title",
    "strong[class*='brand'], span[class*='brand'], em[class*='brand']"
]

async def extract_brands_from_dom(page):
    # 가능한 컨테이너를 좁혀서 그 안에서만 텍스트 추출
    brands = []
    for cont_sel in BRAND_CONTAINER_CANDIDATES:
        try:
            containers = await page.query_selector_all(cont_sel)
        except Exception:
            containers = []
        for c in containers:
            try:
                html = (await c.inner_html()) or ""
                # 랭킹 영역으로 보이는 컨테이너만 (일간/브랜드/순위 숫자 포함)
                if not re.search(r"(브랜드|랭킹).*(일간|주간|월간)|\b1\b", html):
                    continue
            except Exception:
                pass

            for sels in BRAND_NAME_CANDIDATE_SELECTORS:
                try:
                    nodes = await c.query_selector_all(sels)
                    for n in nodes:
                        try:
                            t = (await n.inner_text()).strip()
                            nb = normalize_brand_text(t)
                            if nb:
                                brands.append(nb)
                        except Exception:
                            pass
                except Exception:
                    pass

            # 로고 alt
            try:
                imgs = await c.query_selector_all("img[alt]")
                for im in imgs:
                    try:
                        alt = (await im.get_attribute("alt")) or ""
                        nb = normalize_brand_text(alt)
                        if nb:
                            brands.append(nb)
                    except Exception:
                        pass
            except Exception:
                pass

    uniq = list(OrderedDict.fromkeys(brands))
    junk = {"브랜드","랭킹","판매","온라인","일간","주간","월간"}
    return [x for x in uniq if x not in junk][:100]

# -------------------------
# 크롤링 본체
# -------------------------
async def scrape_top100():
    json_payloads = []

    async with async_playwright() as p:
        iphone = p.devices.get("iPhone 13 Pro")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(**iphone, locale="ko-KR")

        page = await context.new_page()

        # 네트워크 응답 수집 (brand/rank/best 키워드 우선)
        def _want(url: str) -> bool:
            u = url.lower()
            return ("brand" in u or "ranking" in u or "best" in u) and ("oliveyoung" in u or u.startswith("https://"))
        page.on("response", lambda resp: None)  # placeholder for type
        async def on_response(resp):
            try:
                url = resp.url
                if not _want(url):
                    return
                ctype = resp.headers.get("content-type","").lower()
                if "application/json" in ctype:
                    jo = await resp.json()
                    json_payloads.append(jo)
            except Exception:
                pass
        page.on("response", lambda r: asyncio.create_task(on_response(r)))

        await page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_timeout(1000)
        await close_banners(page)
        await maybe_click_brand_tab(page)
        await page.wait_for_timeout(600)

        await click_more_until_end(page)
        await scroll_to_bottom(page, pause_ms=900, max_loops=60)
        await page.wait_for_timeout(1000)  # 네트워크 여유

        brands = extract_brands_from_json_objs(json_payloads)

        if len(brands) < 10:
            # JSON에서 충분치 않으면 DOM 백업 사용
            brands = await extract_brands_from_dom(page)

        # 디버그 아웃풋
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        try:
            await page.screenshot(path=os.path.join(OUTPUT_DIR, "brand_debug.png"), full_page=True)
            with open(os.path.join(OUTPUT_DIR, "brand_debug.json"), "w", encoding="utf-8") as f:
                json.dump(json_payloads[:2], f, ensure_ascii=False, indent=2)  # 일부만 저장
        except Exception:
            pass

        await context.close()
        await browser.close()
        return brands[:100]

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
    ws["A1"] = "브랜드 순위 (올리브영 앱 기준)"
    ws["A1"].font = Font(bold=True, size=12)
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=1 + last_day)

    ws["A2"] = "일자"
    for d in range(1, last_day + 1):
        ws.cell(row=2, column=1 + d).value = f"{d}일"

    ws["A3"] = "요일"
    for d in range(1, last_day + 1):
        wd = date(dt.year, dt.month, d).weekday()
        ws.cell(row=3, column=1 + d).value = ["월","화","수","목","금","토","일"][wd]

    ws["A4"] = "비고"
    for r in range(1, 101):
        ws.cell(row=4 + r, column=1).value = r

    for r in range(1, 5 + 100):
        for c in range(1, 1 + last_day + 1):
            ws.cell(row=r, column=c).alignment = Alignment(vertical="center")
    ws.column_dimensions["A"].width = 8
    for d in range(1, last_day + 1):
        ws.column_dimensions[get_column_letter(1 + d)].width = 18

def write_today(ws, now: datetime, brands):
    col = 1 + now.day
    for i in range(100):
        ws.cell(row=5 + i, column=col).value = brands[i] if i < len(brands) else None

def read_rank_map(ws, day: int):
    col = 1 + day
    ranks = {}
    for i in range(100):
        name = ws.cell(row=5 + i, column=col).value
        if name:
            ranks[str(name).strip()] = i + 1
    return ranks

def get_yesterday_rank_map(wb, now: datetime):
    y = now - timedelta(days=1)
    sheet_name_y = month_sheet_name(y)
    if sheet_name_y in wb.sheetnames:
        ws_y = wb[sheet_name_y]
        try:
            return read_rank_map(ws_y, y.day)
        except Exception:
            pass
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
    if ws.cell(row=2, column=2).value is None:
        setup_layout(ws, now)

    ymap = get_yesterday_rank_map(wb, now)
    write_today(ws, now, brands)
    wb.save(OUTPUT_PATH)
    return ymap, now

# -------------------------
# Slack: Top10 알림 (전일 대비 등락)
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
        r = requests.post(SLACK_WEBHOOK_URL, data=json.dumps(payload),
                          headers={"Content-Type":"application/json"}, timeout=10)
        r.raise_for_status()
        print("[슬랙] Top10 전송 완료")
    except Exception as e:
        print(f"[슬랙] 전송 실패: {e}")

# -------------------------
# Google Drive 업로드 (drive.file 스코프)
# -------------------------
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN and GDRIVE_FOLDER_ID):
        print("[경고] 구글 드라이브 시크릿이 없어 업로드를 건너뜁니다.")
        return None
    try:
        creds = Credentials(
            token=None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            scopes=["https://www.googleapis.com/auth/drive.file"],
        )
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        print(f"[드라이브] 서비스 생성 실패: {e}")
        return None

def find_file_in_folder(service, folder_id, name):
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, fields="files(id, name)", pageSize=1).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None

def upload_or_update_to_drive(filepath, folder_id):
    service = build_drive_service()
    if not service:
        return
    try:
        file_id = find_file_in_folder(service, folder_id, os.path.basename(filepath))
        media = MediaFileUpload(
            filepath,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            resumable=True,
        )
        if file_id:
            service.files().update(fileId=file_id, media_body=media).execute()
            print(f"[드라이브] 기존 파일 갱신 완료: {filepath}")
        else:
            file_metadata = {"name": os.path.basename(filepath), "parents": [folder_id]}
            service.files().create(body=file_metadata, media_body=media, fields="id").execute()
            print(f"[드라이브] 새 파일 업로드 완료: {filepath}")
    except Exception as e:
        print(f"[드라이브] 업로드/갱신 실패: {e}")

# -------------------------
# main
# -------------------------
async def main():
    brands = await scrape_top100()
    if not brands:
        print("[경고] 브랜드 0개 수집 — 디버그 파일 확인 필요")
    else:
        print(f"[INFO] 브랜드 {len(brands)}개 수집")

    ymap, now = save_excel_and_get_yesterday_map(brands)
    post_slack_top10(brands, ymap, now)
    upload_or_update_to_drive(OUTPUT_PATH, GDRIVE_FOLDER_ID)

if __name__ == "__main__":
    asyncio.run(main())
