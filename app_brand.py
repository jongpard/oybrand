# -*- coding: utf-8 -*-
"""
올리브영 모바일 '브랜드 랭킹' 크롤러
- ScraperAPI/ZenRows 등 스크래핑 API 우선 사용 (무료/체험 크레딧 활용)
- 실패 시 Playwright(모바일 에뮬 + 선택적 프록시/쿠키) 폴백
- 엑셀: 월 시트 자동 생성, 일자 열 덮어쓰기 갱신
- 슬랙 Top10 (전일 대비 등락), 구글 드라이브 업로드
"""

import asyncio
import os
import re
import json
import urllib.parse
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
# 기본 설정
# -------------------------
KST = ZoneInfo("Asia/Seoul")
URL = "https://m.oliveyoung.co.kr/m/mtn?menu=ranking&tab=brands"

OUTPUT_DIR = "data"
XLSX_NAME = "올리브영_브랜드_순위.xlsx"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, XLSX_NAME)

# Secrets (환경변수)
SLACK_WEBHOOK_URL    = os.environ.get("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID     = os.environ.get("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "")

# Scraping API
SCRAPING_API         = os.environ.get("SCRAPING_API", "").lower()   # "scraperapi" | "zenrows"
SCRAPING_API_KEY     = os.environ.get("SCRAPING_API_KEY", "")

# (옵션) Cloudflare 회피 보조
PROXY_SERVER         = os.environ.get("PROXY_SERVER", "")      # 예: http://user:pass@host:port
CF_CLEARANCE         = os.environ.get("CF_CLEARANCE", "")

# -------------------------
# 유틸/정규화
# -------------------------
CODE_PATTERNS = [
    re.compile(r"^[A-Z]\d{4,}$"),  # A000688 등
    re.compile(r"^\d{4,}$"),       # 순수 숫자 긴 코드
]

def normalize_brand_text(t: str) -> str | None:
    """브랜드명 후보 텍스트 정규화(숫자/코드/꼬리표 제거, 길이 제한 등)"""
    if not isinstance(t, str):
        return None
    s = re.sub(r"\s+", " ", t).strip()
    if not s:
        return None
    s = re.sub(r"\s*(브랜드\s*썸네일|로고.*|이미지.*|타이틀.*)$", "", s).strip()
    for p in CODE_PATTERNS:
        if p.match(s):
            return None
    if re.search(r"\d", s):
        return None
    if len(s) > 30 or len(s.split()) > 6:
        return None
    if len(s) == 1 and not re.fullmatch(r"[가-힣]", s):
        return None
    if s.lower() in {"brand", "logo", "image", "title"}:
        return None
    return s

# -------------------------
# Scraping API
# -------------------------
def fetch_html_via_scraping_api(url: str) -> str | None:
    """스크래핑 API로 렌더된 HTML 가져오기 (설정돼 있으면 우선 사용)"""
    if not (SCRAPING_API and SCRAPING_API_KEY):
        return None

    enc = urllib.parse.quote_plus(url)

    if SCRAPING_API == "zenrows":
        api_url = (
            f"https://proxy.zenrows.com/?apikey={SCRAPING_API_KEY}&url={enc}"
            f"&js_render=true&premium_proxy=true&country=kr"
        )
    elif SCRAPING_API == "scraperapi":
        # 플랜에 따라 country/device가 제한되면 해당 파라미터는 제거해도 작동 가능
        api_url = (
            f"https://api.scraperapi.com/?api_key={SCRAPING_API_KEY}&url={enc}"
            f"&render=true&country_code=kr&device_type=mobile&session_number=1&retry_404=true"
        )
    else:
        return None

    try:
        r = requests.get(api_url, timeout=40)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[ScrapingAPI] 요청 실패: {e}")
        return None

def extract_brands_from_html(html: str) -> list[str]:
    """렌더된 HTML에서 script JSON을 정규식으로 스캔하여 brandsInfo.brandName만 추출"""
    if not html:
        return []
    names = []
    # 대표 패턴: ... "brandsInfo": { ..., "brandName": "메디힐", ... }
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
    return list(OrderedDict.fromkeys(names))[:100]

# -------------------------
# Playwright helpers (폴백용)
# -------------------------
def parse_proxy(proxy_url: str) -> dict | None:
    if not proxy_url:
        return None
    from urllib.parse import urlparse
    u = urlparse(proxy_url)
    if not (u.scheme and u.hostname and u.port):
        return None
    proxy = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        proxy["username"] = u.username
    if u.password:
        proxy["password"] = u.password
    return proxy

async def maybe_click_brand_tab(page):
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
                await page.wait_for_timeout(400)
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

def is_cf_block_html(html: str) -> bool:
    return bool(html) and (("사람인지 확인" in html) or ("Cloudflare" in html and "확인" in html))

def extract_brands_from_json_objs(json_objs):
    """네트워크 JSON에서 data[*].brandsInfo.brandName만 추출"""
    names = []
    def walk(node):
        if isinstance(node, dict):
            bi = node.get("brandsInfo") or node.get("brandInfo")
            if isinstance(bi, dict):
                nm = (
                    bi.get("brandName") or bi.get("brandNm")
                    or bi.get("brandKrName") or bi.get("brand_kor_name")
                )
                nm = normalize_brand_text(nm)
                if nm:
                    names.append(nm)
            for v in node.values():
                if isinstance(v, (dict, list)):
                    walk(v)
        elif isinstance(node, list):
            for it in node:
                walk(it)
    for jo in json_objs:
        try:
            data = jo.get("data")
            if isinstance(data, list):
                for item in data:
                    bi = item.get("brandsInfo") or item.get("brandInfo")
                    if isinstance(bi, dict):
                        nm = (
                            bi.get("brandName") or bi.get("brandNm")
                            or bi.get("brandKrName") or bi.get("brand_kor_name")
                        )
                        nm = normalize_brand_text(nm)
                        if nm:
                            names.append(nm)
            walk(jo)
        except Exception:
            pass
    return list(OrderedDict.fromkeys(names))[:100]

# -------------------------
# 크롤링 본체
# -------------------------
async def scrape_top100():
    # 0) 스크래핑 API 우선 시도 (내 PC 없이 동작)
    html = fetch_html_via_scraping_api(URL)
    if html:
        brands = extract_brands_from_html(html)
        if brands:
            print("[ScrapingAPI] HTML 추출 성공")
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            with open(os.path.join(OUTPUT_DIR, "brand_debug.html"), "w", encoding="utf-8") as f:
                f.write(html[:200000])
            return brands
        else:
            print("[ScrapingAPI] HTML 추출 실패 - Playwright 폴백 시도")

    # 1) (API 미설정/실패 시) Playwright 폴백
    json_payloads = []
    proxy = parse_proxy(PROXY_SERVER)
    async with async_playwright() as p:
        iphone = p.devices.get("iPhone 13 Pro")
        browser = await p.chromium.launch(headless=True, proxy=proxy)
        context = await browser.new_context(**iphone, locale="ko-KR")
        # cf_clearance 주입(옵션)
        if CF_CLEARANCE:
            try:
                await context.add_cookies([{
                    "name": "cf_clearance",
                    "value": CF_CLEARANCE,
                    "domain": ".oliveyoung.co.kr",
                    "path": "/",
                    "secure": True,
                }])
                print("[CF] cf_clearance 쿠키 주입 완료")
            except Exception as e:
                print(f"[CF] 쿠키 주입 실패: {e}")

        page = await context.new_page()

        async def on_response(resp):
            try:
                url = resp.url.lower()
                ctype = resp.headers.get("content-type","").lower()
                if ("brand" in url or "ranking" in url or "best" in url) and "application/json" in ctype:
                    jo = await resp.json()
                    json_payloads.append(jo)
            except Exception:
                pass
        page.on("response", lambda r: asyncio.create_task(on_response(r)))

        await page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_timeout(800)
        await close_banners(page)
        await maybe_click_brand_tab(page)
        await page.wait_for_timeout(400)

        await click_more_until_end(page)
        await scroll_to_bottom(page, pause_ms=900, max_loops=60)
        await page.wait_for_timeout(800)

        html2 = await page.content()
        if is_cf_block_html(html2):
            print("[경고] Cloudflare 차단 페이지 감지 — API/프록시/국내 IP 필요")

        brands = extract_brands_from_json_objs(json_payloads)
        if len(brands) < 50:
            # 폴백으로 HTML에서라도 추출 시도
            brands_html = extract_brands_from_html(html2)
            brands = list(OrderedDict.fromkeys(brands + brands_html))[:100]

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        try:
            await page.screenshot(path=os.path.join(OUTPUT_DIR, "brand_debug.png"), full_page=True)
            with open(os.path.join(OUTPUT_DIR, "brand_debug.json"), "w", encoding="utf-8") as f:
                json.dump(json_payloads[:3], f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        await context.close()
        await browser.close()
        return brands

# -------------------------
# 엑셀(월 시트 자동 생성/오늘 열 갱신)
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

def post_slack_top10(brands, ymap, now):
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
                          headers={"Content-Type":"application/json"}, timeout=12)
        r.raise_for_status()
        print("[슬랙] Top10 전송 완료")
    except Exception as e:
        print(f"[슬랙] 전송 실패: {e}")

# -------------------------
# 구글 드라이브 업로드 (drive.file 스코프)
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
        print("[경고] 브랜드 0개 수집 — API 키/크레딧 또는 차단 상태 확인 필요")
    else:
        print(f"[INFO] 브랜드 {len(brands)}개 수집")

    ymap, now = save_excel_and_get_yesterday_map(brands)
    post_slack_top10(brands, ymap, now)
    upload_or_update_to_drive(OUTPUT_PATH, GDRIVE_FOLDER_ID)

if __name__ == "__main__":
    asyncio.run(main())
