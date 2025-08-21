# -*- coding: utf-8 -*-
"""
올리브영 모바일 '브랜드 랭킹' 수집 (Oxylabs Realtime API 사용)
- 렌더된 HTML을 API로 받아 brandsInfo.brandName만 추출(Top100)
- 엑셀: 월 시트 자동 생성, 매일 열 덮어쓰기 갱신
- 슬랙: Top10 (전일 대비 등락 표기)
- 구글 드라이브 업로드(선택)
"""

import os
import re
import json
import requests
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from calendar import monthrange
from collections import OrderedDict

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Font
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

# 시크릿/환경변수
SLACK_WEBHOOK_URL    = os.environ.get("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID     = os.environ.get("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "")

SCRAPING_API         = os.environ.get("SCRAPING_API", "").lower()   # 반드시 "oxylabs"
OXY_USER             = os.environ.get("OXY_USER", "")
OXY_PASS             = os.environ.get("OXY_PASS", "")

# -------------------------
# 유틸/정규화
# -------------------------
CODE_PATTERNS = [
    re.compile(r"^[A-Z]\d{4,}$"),  # A000688 등
    re.compile(r"^\d{4,}$"),       # 숫자 긴 코드
]

def normalize_brand_text(t: str) -> str | None:
    if not isinstance(t, str):
        return None
    s = re.sub(r"\s+", " ", t).strip()
    if not s:
        return None
    # 코드/숫자/불필요 꼬리표 컷
    for p in CODE_PATTERNS:
        if p.match(s):
            return None
    if re.search(r"\d", s):
        return None
    s = re.sub(r"\s*(브랜드\s*썸네일|로고.*|이미지.*|타이틀.*)$", "", s).strip()
    if len(s) > 30 or len(s.split()) > 6:
        return None
    if len(s) == 1 and not re.fullmatch(r"[가-힣]", s):
        return None
    if s.lower() in {"brand","logo","image","title"}:
        return None
    return s

# -------------------------
# Oxylabs Realtime API
# -------------------------
def fetch_html_via_oxylabs(url: str) -> str | None:
    if SCRAPING_API != "oxylabs" or not (OXY_USER and OXY_PASS):
        print("[Oxylabs] 시크릿이 설정되지 않았습니다.")
        return None
    try:
        payload = {
            "source": "universal",
            "url": url,
            "render": "html",                # JS 렌더링
            "geo_location": "South Korea",   # 한국 지리
            "user_agent_type": "mobile"      # 모바일 UA
        }
        r = requests.post(
            "https://realtime.oxylabs.io/v1/queries",
            auth=(OXY_USER, OXY_PASS),
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        results = data.get("results") or []
        if results and "content" in results[0]:
            return results[0]["content"]
        print("[Oxylabs] content 없음")
    except Exception as e:
        print(f"[Oxylabs] 요청 실패: {e}")
    return None

def extract_brands_from_html(html: str) -> list[str]:
    """렌더된 HTML에서 brandsInfo.brandName만 추출(순서=랭킹)"""
    if not html:
        return []
    names = []
    # 대표 패턴: "brandsInfo": { ... "brandName": "메디힐" ... }
    for m in re.finditer(r'brandsInfo"\s*:\s*{[^}]*"brandName"\s*:\s*"([^"]+)"', html):
        nm = normalize_brand_text(m.group(1))
        if nm:
            names.append(nm)
    # 백업: brandName 키 전체 탐색
    if not names:
        for m in re.finditer(r'"brandName"\s*:\s*"([^"]+)"', html):
            nm = normalize_brand_text(m.group(1))
            if nm:
                names.append(nm)
    # 순서 유지·중복 제거
    return list(OrderedDict.fromkeys(names))[:100]

# -------------------------
# 엑셀 (월 시트 자동 생성/오늘 열 갱신)
# -------------------------
def month_sheet_name(dt: datetime) -> str:
    return f"{dt.strftime('%y')}년 {dt.month}월"

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

def write_today(ws, now: datetime, brands: list[str]):
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

def save_excel_and_get_yesterday_map(brands: list[str]):
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
# 슬랙 Top10
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

def post_slack_top10(brands: list[str], ymap: dict, now: datetime):
    if not SLACK_WEBHOOK_URL:
        print("[경고] SLACK_WEBHOOK_URL 미설정 — 슬랙 전송 생략")
        return
    if not brands:
        print("[슬랙] 수집 결과 0개 — 전송 생략")
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
                          headers={"Content-Type": "application/json"}, timeout=12)
        r.raise_for_status()
        print("[슬랙] Top10 전송 완료")
    except Exception as e:
        print(f"[슬랙] 전송 실패: {e}")

# -------------------------
# 구글 드라이브 업로드 (선택)
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
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    html = fetch_html_via_oxylabs(URL)
    if html:
        # 디버그 저장(확인용)
        try:
            with open(os.path.join(OUTPUT_DIR, "brand_debug.html"), "w", encoding="utf-8") as f:
                f.write(html[:200000])
        except Exception:
            pass
        brands = extract_brands_from_html(html)
    else:
        brands = []

    if not brands:
        print("[경고] 브랜드 0개 수집 — Oxylabs 설정/응답 확인 필요")
    else:
        print(f"[INFO] 브랜드 {len(brands)}개 수집")

    ymap, now = save_excel_and_get_yesterday_map(brands)
    post_slack_top10(brands, ymap, now)
    upload_or_update_to_drive(OUTPUT_PATH, GDRIVE_FOLDER_ID)

if __name__ == "__main__":
    main()
