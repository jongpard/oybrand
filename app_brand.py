# -*- coding: utf-8 -*-
# Olive Young 모바일 브랜드 랭킹(1~100) 수집 -> 월별 시트 Excel 저장 -> Google Drive 업로드 -> Slack 알림
# - 월이 바뀌면 같은 파일 내 새 시트 생성(예: "25년 9월")
# - 연도가 바뀌면 연도별 새 파일 생성(예: "올리브영_브랜드_랭킹_2026.xlsx")
# - 레포 시크릿 사용: SLACK_WEBHOOK_URL, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN, GDRIVE_FOLDER_ID

import os
import re
import io
import json
import logging
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# Excel
from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

# Google Drive (OAuth2, 사용자 계정)
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# Playwright (모바일 렌더링)
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except Exception:
    PLAYWRIGHT_AVAILABLE = False

# -------------------- 환경변수 --------------------
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "").strip()
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()

TARGET_URL = "https://m.oliveyoung.co.kr/m/mtn?menu=ranking&tab=brands"
OUT_DIR = "out"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


# -------------------- 공통 유틸 --------------------
def kst_now():
    return datetime.now(timezone.utc) + timedelta(hours=9)

def make_session_mobile():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        # 모바일 UA
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
        ),
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://m.oliveyoung.co.kr/",
    })
    return s


# -------------------- Slack --------------------
def send_slack(text: str):
    if not SLACK_WEBHOOK:
        logging.warning("SLACK_WEBHOOK_URL 미설정")
        return
    try:
        requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=10)
    except Exception:
        logging.exception("Slack 전송 실패")


# -------------------- Google Drive(OAuth) --------------------
def build_drive_service_oauth():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        logging.warning("Google OAuth 환경변수 누락")
        return None
    try:
        creds = UserCredentials(
            None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=["https://www.googleapis.com/auth/drive.file"],
        )
        creds.refresh(GoogleRequest())
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception:
        logging.exception("Drive 서비스 초기화 실패")
        return None

def find_file_by_name(service, folder_id: str, name: str):
    try:
        if folder_id:
            q = f"name='{name}' and '{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
        else:
            q = f"name='{name}' and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
        res = service.files().list(q=q, pageSize=1, fields="files(id,name)").execute()
        files = res.get("files", [])
        return files[0] if files else None
    except Exception:
        logging.exception("find_file_by_name 실패")
        return None

def download_file_bytes(service, file_id: str) -> bytes | None:
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read()
    except Exception:
        logging.exception("Drive 파일 다운로드 실패")
        return None

def upload_xlsx(service, xlsx_bytes: bytes, filename: str, folder_id: str):
    try:
        media = MediaIoBaseUpload(io.BytesIO(xlsx_bytes),
                                  mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                  resumable=False)
        body = {"name": filename}
        if folder_id:
            body["parents"] = [folder_id]
        return service.files().create(body=body, media_body=media, fields="id,name,webViewLink").execute()
    except Exception:
        logging.exception("Drive 업로드 실패")
        return None

def update_xlsx(service, file_id: str, xlsx_bytes: bytes):
    try:
        media = MediaIoBaseUpload(io.BytesIO(xlsx_bytes),
                                  mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                  resumable=False)
        return service.files().update(fileId=file_id, media_body=media).execute()
    except Exception:
        logging.exception("Drive 업데이트 실패")
        return None


# -------------------- 스크랩(브랜드 100) --------------------
# 1) 혹시 존재할 수 있는 비공개/내부 API 추정 엔드포인트를 먼저 시도(실패 시 무시)
def try_mobile_api_candidates(max_items=100):
    session = make_session_mobile()
    candidates = [
        # 올영 모바일에서 쓰일 법한 후보들(변동 가능). 실패해도 조용히 패스.
        ("getBestBrandList", "https://m.oliveyoung.co.kr/m/api/best/getBestBrandList.do", {}),
        ("getBrandRankingList", "https://m.oliveyoung.co.kr/m/api/best/getBrandRankingList.do", {}),
        ("brandRankList", "https://m.oliveyoung.co.kr/m/api/best/brandRankList.do", {}),
    ]
    for name, url, params in candidates:
        try:
            r = session.get(url, params=params, timeout=10)
            if r.status_code != 200:
                continue
            data = r.json()
            # 일반적인 키 후보
            for k in ["list", "rows", "items", "brandList", "result", "data"]:
                arr = data.get(k)
                if isinstance(arr, list) and arr:
                    brands = []
                    for it in arr:
                        b = it.get("brandNm") or it.get("brandName") or it.get("name") or it.get("nm")
                        if b and b.strip():
                            brands.append(b.strip())
                        if len(brands) >= max_items:
                            break
                    if brands:
                        logging.info("API(%s)에서 %d개 추출", name, len(brands))
                        return brands[:max_items]
        except Exception:
            continue
    return None

# 2) HTML 렌더링(모바일) 후 추출 — 가장 안정적인 안전장치
def scrape_mobile_html(max_items=100):
    if not PLAYWRIGHT_AVAILABLE:
        logging.warning("Playwright 미설치")
        return None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            device = p.devices.get("iPhone 12") or {}
            context = browser.new_context(**device, locale="ko-KR")
            page = context.new_page()
            page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(1500)

            # li 를 많이 포함하는 컨테이너를 골라 그 하위에서 브랜드 텍스트를 수집
            containers = page.query_selector_all("section, div, ul, ol")
            best = None
            best_cnt = 0
            for c in containers:
                try:
                    cnt = c.eval_on_selector_all("li", "els => els.length")
                    if cnt and cnt > best_cnt:
                        best = c
                        best_cnt = cnt
                except Exception:
                    pass

            # 하위 li를 위에서부터 훑으며 브랜드명 후보 탐색
            names = []
            if best:
                lis = best.query_selector_all("li")
            else:
                lis = page.query_selector_all("li")

            def pick_name(li):
                # 브랜드명처럼 보이는 텍스트를 우선순위로 선택
                for sel in [".brand_name", ".tx_brand", ".brand", "strong", "span", "a"]:
                    el = li.query_selector(sel)
                    if el:
                        t = (el.inner_text() or "").strip()
                        if is_brand_like(t):
                            return t
                # 대체: li 전체 텍스트에서 한 줄 추출
                t = (li.inner_text() or "").strip().split("\n")[0].strip()
                t = re.sub(r"\s{2,}", " ", t)
                if is_brand_like(t):
                    return t
                return None

            for li in lis:
                if len(names) >= max_items:
                    break
                nm = pick_name(li)
                if nm and nm not in names:
                    names.append(nm)

            browser.close()
            logging.info("HTML 렌더링 추출 수: %d", len(names))
            return names[:max_items] if names else None
    except Exception:
        logging.exception("Playwright 렌더링 실패")
        return None

_brand_pat = re.compile(r"^[가-힣A-Za-z0-9 .&+’'\-·/()]+$")

def is_brand_like(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    if len(s) < 1 or len(s) > 40:
        return False
    if "위" in s or "랭킹" in s or "BEST" in s.upper():
        return False
    if not _brand_pat.match(s):
        return False
    # 의미 없는 메뉴/탭 용어 제거
    bad_tokens = ["브랜드", "전체", "카테고리", "필터", "더보기", "로그인", "장바구니"]
    if any(bt in s for bt in bad_tokens):
        return False
    return True

def get_top100_brands():
    # 1차: API 후보
    brands = try_mobile_api_candidates()
    if brands and len(brands) >= 20:
        return brands[:100]
    # 2차: 모바일 렌더링
    return scrape_mobile_html(100) or []


# -------------------- Excel(월 시트) --------------------
def month_sheet_name(dt: datetime) -> str:
    yy = dt.year % 100
    mm = dt.month
    return f"{yy}년 {mm}월"

def target_filename(dt: datetime) -> str:
    # 연도별 파일명 (예: 올리브영_브랜드_랭킹_2025.xlsx)
    return f"올리브영_브랜드_랭킹_{dt.year}.xlsx"

def ensure_sheet_header(ws):
    if ws.max_row == 1 and ws.max_column == 1 and (ws["A1"].value is None):
        # 완전 신규 시트
        headers = ["날짜"] + [f"{i}위" for i in range(1, 101)]
        ws.append(headers)
        # 약간의 너비 조정
        ws.column_dimensions["A"].width = 14
        for c in range(2, 102):
            ws.column_dimensions[get_column_letter(c)].width = 12

def write_today_row(ws, dt: datetime, brands: list[str]):
    date_str = dt.date().isoformat()
    # 이미 같은 날짜가 있으면 덮어쓰기
    row_idx = None
    for r in range(2, ws.max_row + 1):
        if str(ws.cell(r, 1).value) == date_str:
            row_idx = r
            break
    if row_idx is None:
        row_idx = ws.max_row + 1

    ws.cell(row=row_idx, column=1, value=date_str)
    for i in range(100):
        val = brands[i] if i < len(brands) else ""
        ws.cell(row=row_idx, column=2 + i, value=val)


# -------------------- 메인 --------------------
def main():
    now = kst_now()
    logging.info("모바일 브랜드 랭킹 수집 시작: %s", TARGET_URL)

    brands = get_top100_brands()
    if not brands:
        send_slack("❌ 올리브영 *모바일 브랜드 랭킹* 수집 실패")
        return 1

    # Excel 준비
    service = build_drive_service_oauth()
    fname = target_filename(now)
    file_meta = find_file_by_name(service, GDRIVE_FOLDER_ID, fname)

    wb = None
    if file_meta:
        # 기존 파일 로드
        data = download_file_bytes(service, file_meta["id"])
        if data:
            wb = load_workbook(io.BytesIO(data))
    if wb is None:
        wb = Workbook()

    # 시트 준비
    sheet_name = month_sheet_name(now)
    if sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
    else:
        ws = wb.create_sheet(title=sheet_name)
    # 기본 생성 시 생기는 'Sheet' 제거(빈 통합문서 최초 생성 시)
    if "Sheet" in wb.sheetnames and len(wb.sheetnames) > 1:
        try:
            wb.remove(wb["Sheet"])
        except Exception:
            pass

    ensure_sheet_header(ws)
    write_today_row(ws, now, brands)

    # 로컬 저장
    os.makedirs(OUT_DIR, exist_ok=True)
    local_path = os.path.join(OUT_DIR, fname)
    wb.save(local_path)

    # 업로드/업데이트
    with open(local_path, "rb") as f:
        xbytes = f.read()

    if file_meta:
        update_xlsx(service, file_meta["id"], xbytes)
        view_link = None
        try:
            meta = service.files().get(fileId=file_meta["id"], fields="webViewLink").execute()
            view_link = meta.get("webViewLink")
        except Exception:
            pass
        logging.info("업데이트 완료: %s", fname)
    else:
        created = upload_xlsx(service, xbytes, fname, GDRIVE_FOLDER_ID)
        view_link = (created or {}).get("webViewLink")
        logging.info("신규 업로드 완료: %s", fname)

    # Slack 요약(Top 20만 표시)
    top20 = "\n".join([f"{i+1}. {brands[i]}" for i in range(min(20, len(brands)))])
    msg = (
        f"*올리브영 모바일 브랜드 랭킹 100* ({now.strftime('%Y-%m-%d %H:%M KST')})\n"
        f"- 월 시트: `{sheet_name}` / 파일: `{fname}`\n"
        f"- 상위 20\n{top20}"
    )
    if view_link:
        msg += f"\n<{view_link}|Google Drive에서 열기>"
    send_slack(msg)

    logging.info("완료")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
