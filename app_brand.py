# -*- coding: utf-8 -*-
# 올리브영 모바일 '브랜드 랭킹' Top100 수집 → 월별 시트 Excel 저장 → Google Drive 업로드 → Slack 알림(아마존 스타일)

import os, io, re, logging
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# Playwright + stealth
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    from playwright_stealth import stealth_sync
    PW_OK = True
except Exception:
    PW_OK = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "").strip()
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()

TARGET_URL = "https://m.oliveyoung.co.kr/m/mtn?menu=ranking&tab=brands"
OUT_DIR = "out"

def kst_now():
    return datetime.now(timezone.utc) + timedelta(hours=9)

def make_session_mobile():
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1,
                                                     status_forcelist=[429,500,502,503,504])))
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
                       "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://m.oliveyoung.co.kr/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    return s

def send_slack(text: str):
    if not SLACK_WEBHOOK: return
    try:
        requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=10)
    except Exception:
        logging.exception("Slack 전송 실패")

# -------- Drive --------
def drive_service():
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
        logging.exception("Drive 초기화 실패"); return None

def drive_find(svc, folder_id, name):
    try:
        q = f"name='{name}' and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
        if folder_id: q += f" and '{folder_id}' in parents"
        r = svc.files().list(q=q, pageSize=1, fields="files(id,name)").execute()
        fs = r.get("files", [])
        return fs[0] if fs else None
    except Exception:
        logging.exception("Drive find 실패"); return None

def drive_download(svc, file_id):
    try:
        req = svc.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        dl = MediaIoBaseDownload(buf, req)
        done = False
        while not done:
            _, done = dl.next_chunk()
        buf.seek(0); return buf.read()
    except Exception:
        logging.exception("Drive 다운로드 실패"); return None

def drive_upload_new(svc, folder_id, filename, data: bytes):
    try:
        media = MediaIoBaseUpload(io.BytesIO(data),
                                  mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                  resumable=False)
        body = {"name": filename}
        if folder_id: body["parents"] = [folder_id]
        return svc.files().create(body=body, media_body=media, fields="id,webViewLink").execute()
    except Exception:
        logging.exception("Drive 업로드 실패"); return None

def drive_update(svc, file_id, data: bytes):
    try:
        media = MediaIoBaseUpload(io.BytesIO(data),
                                  mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                  resumable=False)
        return svc.files().update(fileId=file_id, media_body=media).execute()
    except Exception:
        logging.exception("Drive 업데이트 실패"); return None

# -------- 수집 --------
_RANK_PAT = re.compile(r"(^|\s)(\d{1,3})\s*위(?![^\n]*OUT)", re.M)
_BAD = set(["브랜드","더보기","장바구니","로그인","고객센터","대표전화","채팅","사업자","개인정보",
            "청소년","법적고지","인스타그램","페이스북","유튜브","카카오톡","이용약관",
            "공지","쿠폰","혜택","증정","무배","오늘드림","1+1","업데이트 확인"])

def is_brand_like(s: str) -> bool:
    if not s: return False
    s = s.strip()
    if len(s) < 1 or len(s) > 40: return False
    if any(t in s for t in _BAD): return False
    return True

def _extract_from_li(li):
    try:
        txt = (li.inner_text() or "").strip()
        m = _RANK_PAT.search(txt)
        rk = int(m.group(2)) if m else None
        for sel in [".brand_name",".brand-name",".brandNm",".tx_brand",".name",".tit",".title","strong"]:
            el = li.query_selector(sel)
            if el:
                nm = (el.inner_text() or "").strip()
                nm = re.sub(r"\s{2,}", " ", nm)
                if is_brand_like(nm): return rk, nm
        for sel in ["img[alt]","[aria-label]"]:
            el = li.query_selector(sel)
            if el:
                v = (el.get_attribute("alt") or el.get_attribute("aria-label") or "").strip()
                v = re.sub(r"\b\d{1,3}\s*위\b", "", v).strip()
                if is_brand_like(v): return rk, v
        for line in [x.strip() for x in txt.split("\n") if x.strip()]:
            line = re.sub(r"\b\d{1,3}\s*위\b", "", line).strip()
            if is_brand_like(line): return rk, line
        return rk, None
    except Exception:
        return None, None

def sniff_brands_from_network(page, timeout_ms=5000):
    found = []
    def handler(resp):
        try:
            ctype = (resp.headers.get("content-type") or "").lower()
            if "application/json" not in ctype: return
            data = resp.json()
            for key in ["list","rows","items","brandList","data","result"]:
                arr = data.get(key)
                if isinstance(arr, list) and arr:
                    tmp = []
                    for it in arr:
                        nm = (it.get("brandNm") or it.get("brandName") or it.get("nm") or it.get("name"))
                        rk = (it.get("rank") or it.get("rk") or it.get("ord"))
                        if nm:
                            tmp.append((int(rk) if rk else 9999, str(nm).strip()))
                    if tmp:
                        tmp.sort(key=lambda x: x[0])
                        names = [nm for rk, nm in tmp if 1 <= rk <= 100]
                        if names: found.extend(names)
        except Exception:
            pass
    page.on("response", handler)
    page.wait_for_timeout(timeout_ms)
    out, used = [], set()
    for nm in found:
        if nm and nm not in used:
            used.add(nm); out.append(nm)
            if len(out) >= 100: break
    return out

def try_playwright(max_items=100):
    if not PW_OK: return None
    try:
        with sync_playwright() as p:
            for which in ("chromium","webkit"):
                browser = None
                try:
                    if which == "chromium":
                        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
                        device = p.devices.get("Pixel 5") or {}
                    else:
                        browser = p.webkit.launch(headless=True)
                        device = p.devices.get("iPhone 12") or {}
                    context = browser.new_context(**device, locale="ko-KR")
                    page = context.new_page()
                    stealth_sync(page)  # 차단 완화
                    page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)

                    # 상단 쿠폰/팝업 닫기 시도
                    for sel in ["button[aria-label*='닫기']", "button.close", "div[role='dialog'] button", "button:has-text('닫기')"]:
                        try: page.locator(sel).first.click(timeout=1000)
                        except Exception: pass

                    # '브랜드 랭킹' 탭 클릭 (URL 파라미터가 있어도 실제 클릭해야 XHR 뜨는 경우 대응)
                    try:
                        page.get_by_text("브랜드 랭킹", exact=False).first.click(timeout=2000)
                        page.wait_for_load_state("networkidle", timeout=10000)
                    except Exception:
                        pass

                    # 스크롤 로딩
                    for _ in range(6):
                        page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                        page.wait_for_timeout(350)

                    names = sniff_brands_from_network(page, 5000)

                    if len(names) < 20:
                        # DOM 보조 추출
                        items = []
                        for c in page.query_selector_all("main, section, div, ul, ol"):
                            try:
                                t = (c.inner_text() or "")
                                if ("브랜드" in t and ("랭킹" in t or "순위" in t)) or _RANK_PAT.search(t):
                                    for li in c.query_selector_all("li"):
                                        rk, nm = _extract_from_li(li)
                                        if rk and nm and 1 <= rk <= 100 and is_brand_like(nm):
                                            items.append((rk, nm))
                            except Exception:
                                pass
                        by_rank = {}
                        for rk, nm in items:
                            if rk not in by_rank: by_rank[rk] = nm
                        names = [by_rank.get(i, "") for i in range(1, 101)]
                        names = [x for x in names if x]

                    if names:
                        logging.info("Playwright(%s)에서 %d개 추출", which, len(names))
                        return names[:max_items]
                except Exception as e:
                    logging.warning("Playwright %s 실패: %s", which, e)
                finally:
                    if browser:
                        try: browser.close()
                        except Exception: pass
    except Exception:
        logging.exception("Playwright 단계 전체 실패")
    return None

def try_requests_html(max_items=100):
    try:
        s = make_session_mobile()
        r = s.get(TARGET_URL, timeout=15)
        if r.status_code == 403:
            logging.warning("Requests 단계 403 차단"); return None
        r.raise_for_status()
        html = r.text
        pairs = []
        for mm in re.finditer(r'"(brandNm|brandName|name)"\s*:\s*"([^"]+)"\s*,\s*"(rank|rk|ord)"\s*:\s*"?(\\d{1,3})"?', html):
            try:
                nm = mm.group(2); rk = int(mm.group(4))
                if 1 <= rk <= 100 and is_brand_like(nm): pairs.append((rk, nm.strip()))
            except Exception: pass
        if not pairs:
            soup = BeautifulSoup(html, "html.parser")
            for line in soup.get_text("\n", strip=True).split("\n"):
                m = re.search(r"(\d{1,2})\s*위\s*([가-힣A-Za-z0-9 ·&+\-/'()]+)", line)
                if m:
                    rk = int(m.group(1)); nm = m.group(2).strip()
                    if 1 <= rk <= 100 and is_brand_like(nm):
                        pairs.append((rk, nm))
        if not pairs: return None
        uniq = {}
        for rk, nm in pairs:
            if rk not in uniq: uniq[rk] = nm
        out = [uniq.get(i, "") for i in range(1, 101)]
        out = [x for x in out if x]
        return out[:max_items] if out else None
    except Exception:
        logging.exception("Requests/HTML 단계 실패"); return None

def scrape_top100():
    out = try_playwright()
    if out and len(out) >= 20: return out[:100]
    out = try_requests_html()
    if out and len(out) >= 20: return out[:100]
    return []

# -------- 엑셀 --------
def month_sheet(dt): return f"{dt.year%100}년 {dt.month}월"
def file_name(dt): return f"올리브영_브랜드_랭킹_{dt.year}.xlsx"

def ensure_header(ws):
    if ws.max_row == 1 and ws.max_column == 1 and ws["A1"].value is None:
        ws.append(["날짜"] + [f"{i}위" for i in range(1, 101)])
        ws.column_dimensions["A"].width = 14
        for c in range(2, 102): ws.column_dimensions[get_column_letter(c)].width = 12

def write_today(ws, dt, brands):
    d = dt.date().isoformat()
    row = None
    for r in range(2, ws.max_row + 1):
        if str(ws.cell(r, 1).value) == d: row = r; break
    if row is None: row = ws.max_row + 1
    ws.cell(row=row, column=1, value=d)
    for i in range(100):
        ws.cell(row=row, column=2 + i, value=(brands[i] if i < len(brands) else ""))

def read_prev_map(wb, now_dt):
    def row_to_map(ws, r):
        m = {}
        for i in range(1, 101):
            v = ws.cell(r, 1 + i).value
            if v: m[str(v).strip()] = i
        return m
    cand = []
    cur = month_sheet(now_dt)
    if cur in wb.sheetnames:
        ws = wb[cur]
        for r in range(2, ws.max_row + 1):
            d = str(ws.cell(r, 1).value)
            if d and d < now_dt.date().isoformat(): cand.append((ws, r))
    prev_m = (now_dt.replace(day=1) - timedelta(days=1))
    prev_name = month_sheet(prev_m)
    if prev_name in wb.sheetnames:
        ws2 = wb[prev_name]
        if ws2.max_row >= 2: cand.append((ws2, ws2.max_row))
    if not cand: return {}
    ws, r = cand[-1]; return row_to_map(ws, r)

# -------- Slack 포맷(아마존 스타일) --------
def delta_str(today_rank, prev_rank):
    if prev_rank is None: return "NEW"
    diff = prev_rank - today_rank
    if diff > 0: return f"↑{diff}"
    if diff < 0: return f"↓{abs(diff)}"
    return "—"

def build_sections(today_list, prev_map):
    today_map = {b: i+1 for i, b in enumerate(today_list)}
    top10 = [f"{i+1}. ({delta_str(i+1, prev_map.get(b))}) {b}" for i, b in enumerate(today_list[:10])]
    ups, downs = [], []
    for b, tr in today_map.items():
        pr = prev_map.get(b)
        if pr is None: continue
        diff = pr - tr
        if diff >= 10: ups.append((diff, b, pr, tr))
        elif diff <= -10: downs.append((abs(diff), b, pr, tr))
    ups.sort(key=lambda x: (-x[0], x[3])); downs.sort(key=lambda x: (-x[0], x[3]))
    ups_lines = [f"- {b} {pr}위 → {tr}위 (↑{d})" for d, b, pr, tr in ups[:5]]
    downs_lines = [f"- {b} {pr}위 → {tr}위 (↓{d})" for d, b, pr, tr in downs[:5]]
    newins = [b for b in today_list if b not in prev_map][:5]
    newins_lines = [f"- {b} NEW → {today_map[b]}위" for b in newins]
    prev_top70 = [(b, r) for b, r in prev_map.items() if r <= 70]
    outs = []
    today_set = set(today_list)
    for b, r in sorted(prev_top70, key=lambda x: x[1]):
        if b not in today_set: outs.append((b, r))
    outs_lines = [f"- {b} {r}위 → OUT" for b, r in outs[:5]]
    inout_summary = f"{len(newins)}개 IN, {len(outs)}개 OUT"
    return top10, ups_lines, newins_lines, downs_lines, outs_lines, inout_summary

# -------- 메인 --------
def main():
    now = kst_now()
    logging.info("브랜드 랭킹 수집 시작")

    try:
        brands = scrape_top100()
    except Exception:
        logging.exception("scrape_top100 예외"); brands = []

    if len(brands) < 20:
        send_slack("❌ *올리브영 모바일 브랜드 랭킹* 수집 실패 (차단/로딩 실패). 브라우저 설치 및 차단 정책 확인 필요.")
        return 0  # 실패시에도 워크플로우 성공 처리

    svc = drive_service()
    fname = file_name(now)
    meta = drive_find(svc, GDRIVE_FOLDER_ID, fname)

    wb = None
    if meta:
        data = drive_download(svc, meta["id"])
        if data: wb = load_workbook(io.BytesIO(data))
    if wb is None: wb = Workbook()

    sheet = month_sheet(now)
    ws = wb[sheet] if sheet in wb.sheetnames else wb.create_sheet(title=sheet)
    if "Sheet" in wb.sheetnames and len(wb.sheetnames) > 1:
        try: wb.remove(wb["Sheet"])
        except Exception: pass

    ensure_header(ws)
    prev_map = read_prev_map(wb, now)
    write_today(ws, now, brands)

    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, fname)
    wb.save(path)
    with open(path, "rb") as f:
        xbytes = f.read()

    if meta:
        drive_update(svc, meta["id"], xbytes)
        try:
            view_link = svc.files().get(fileId=meta["id"], fields="webViewLink").execute().get("webViewLink")
        except Exception:
            view_link = None
    else:
        created = drive_upload_new(svc, GDRIVE_FOLDER_ID, fname, xbytes)
        view_link = (created or {}).get("webViewLink")

    top10, ups, newins, downs, outs, inout = build_sections(brands, prev_map)
    msg = [
        f"*올리브영 모바일 브랜드 랭킹 100* — {now.strftime('%Y-%m-%d')}",
        f"- 월 시트: `{sheet}` / 파일: `{fname}`",
        "",
        "*TOP 10*",
        *top10, "",
        "🔥 *급상승* (10계단↑, 최대 5개)" if ups else "🔥 *급상승*: 해당 없음", *ups, "",
        "🆕 *뉴브랜드* (오늘 Top100 신규, 최대 5개)" if newins else "🆕 *뉴브랜드*: 해당 없음", *newins, "",
        "📉 *급하락* (10계단↓, 최대 5개)" if downs else "📉 *급하락*: 해당 없음", *downs, "",
        "⬅️ *랭크 아웃* (전일 Top70 → 금일 OUT, 최대 5개)" if outs else "⬅️ *랭크 아웃*: 해당 없음", *outs, "",
        f"➡️ 랭크 인&아웃 요약: {inout}",
    ]
    if view_link: msg.append(f"\n<{view_link}|Google Drive에서 열기>")
    send_slack("\n".join(msg))
    logging.info("완료"); return 0

if __name__ == "__main__":
    raise SystemExit(main())
