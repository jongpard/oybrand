#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# app.py — GDrive(OAuth, 사용자 계정) 업로드 + 할인율/전일비 분석(한국시간) + Slack 포맷 개선

import os
import re
import json
import logging
from io import BytesIO, StringIO
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# (optional) Playwright fallback
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except Exception:
    PLAYWRIGHT_AVAILABLE = False

# Google Drive (OAuth)
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ---------------- 설정(ENV)
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "").strip()

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "").strip()
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()

OUT_DIR = "rankings"
MAX_ITEMS = 100        # 크롤링 최대 아이템
TOP_WINDOW = 30        # 인/아웃 판정, 급하락/랭크아웃 기준 윈도우(상위 30)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


# ---------------- 유틸
def kst_now():
    return datetime.now(timezone.utc) + timedelta(hours=9)

def make_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.oliveyoung.co.kr/",
    })
    return s

_won_pat = re.compile(r"[\d,]+")

def parse_won_to_int(s: str | None) -> int | None:
    if not s:
        return None
    m = _won_pat.search(s)
    if not m:
        return None
    try:
        return int(m.group(0).replace(",", ""))
    except Exception:
        return None

def fmt_price_with_discount(sale: int | None, disc_pct: int | None) -> str:
    if not sale:
        return ""
    if disc_pct is None:
        return f"{sale:,}원"
    # 퍼센트 앞에 ↓ 붙이기
    return f"{sale:,}원 (↓{disc_pct}%)"



# ---------------- 파싱/정제
def clean_title(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip()
    s = re.sub(r'^\s*(?:\[[^\]]*\]\s*)+', '', s)  # [ ... ] 제거
    s = re.sub(r'^\s*([^|\n]{1,40}\|\s*)+', '', s)  # 앞단 프로모션 토막 제거
    s = re.sub(r'^\s*(리뷰 이벤트|PICK|오특|이벤트|특가|[^\s]*PICK)\s*[:\-–—]?\s*', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def extract_brand_from_name(name: str) -> str:
    if not name:
        return ""
    parts = re.split(r'[\s·\-–—\/\\\|,]+', name)
    if parts:
        cand = parts[0]
        if re.match(r'^\d|\+|세트|기획', cand):
            return parts[1] if len(parts) > 1 else cand
        return cand
    return name

def parse_html_products(html: str):
    soup = BeautifulSoup(html, "html.parser")
    candidate_selectors = [
        "ul.cate_prd_list li",
        "ul.prd_list li",
        ".cate_prd_list li",
        ".ranking_list li",
        ".rank_item",
    ]
    results = []
    for sel in candidate_selectors:
        els = soup.select(sel)
        if not els:
            continue
        for el in els:
            if len(results) >= MAX_ITEMS:
                break

            # 이름
            name_node = None
            for ns in [".tx_name", ".prd_name .tx_name", ".prd_name", ".prd_tit", "a"]:
                node = el.select_one(ns)
                if node and node.get_text(strip=True):
                    name_node = node
                    break
            if not name_node:
                continue
            raw_name = name_node.get_text(" ", strip=True)
            cleaned = clean_title(raw_name)

            # 가격
            sale_node = el.select_one(".tx_cur .tx_num") or el.select_one(".tx_cur")
            org_node  = el.select_one(".tx_org .tx_num") or el.select_one(".tx_org")
            sale_price = parse_won_to_int(sale_node.get_text(strip=True) if sale_node else "")
            original_price = parse_won_to_int(org_node.get_text(strip=True) if org_node else "")

            # 브랜드
            brand_node = el.select_one(".tx_brand") or el.select_one(".brand")
            brand = brand_node.get_text(strip=True) if brand_node else extract_brand_from_name(cleaned)

            # 링크
            link_node = el.select_one("a")
            href = link_node.get("href") if link_node else None
            if href and href.startswith("/"):
                href = "https://www.oliveyoung.co.kr" + href

            # 할인율
            disc_pct = None
            if original_price and sale_price and original_price > sale_price:
                disc_pct = int((original_price - sale_price) / original_price * 100)

            results.append({
                "raw_name": raw_name,
                "name": cleaned,
                "brand": brand,
                "url": href,
                "original_price": original_price,
                "sale_price": sale_price,
                "discount_pct": disc_pct,
                "rank": None,
            })
        if results:
            logging.info("parse_html_products: %s -> %d개", sel, len(results))
            break
    return results

def try_http_candidates():
    session = make_session()
    candidates = [
        ("getBestList", "https://www.oliveyoung.co.kr/store/main/getBestList.do", {"rowsPerPage": str(MAX_ITEMS), "pageIdx":"0"}),
        ("getBestList_disp_total", "https://www.oliveyoung.co.kr/store/main/getBestList.do", {"dispCatNo":"90000010001", "rowsPerPage": str(MAX_ITEMS), "pageIdx":"0"}),
        ("getTopSellerList", "https://www.oliveyoung.co.kr/store/main/getTopSellerList.do", {"rowsPerPage": str(MAX_ITEMS), "pageIdx":"0"}),
        ("getBestListJson", "https://www.oliveyoung.co.kr/store/main/getBestListJson.do", {"rowsPerPage": str(MAX_ITEMS), "pageIdx":"0"}),
    ]
    for name, url, params in candidates:
        try:
            logging.info("HTTP try: %s %s %s", name, url, params)
            r = session.get(url, params=params, timeout=15)
            logging.info(" -> status=%s, ct=%s, len=%d", r.status_code, r.headers.get("Content-Type"), len(r.text or ""))
            if r.status_code != 200:
                continue
            ct = r.headers.get("Content-Type","")
            text = r.text or ""

            # JSON 스키마 추정
            if "application/json" in ct or text.strip().startswith("{") or text.strip().startswith("["):
                try:
                    data = r.json()
                except Exception:
                    data = None
                if isinstance(data, dict):
                    for k in ["BestProductList", "list", "rows", "items", "bestList", "result"]:
                        if k in data and isinstance(data[k], list) and data[k]:
                            out = []
                            for it in data[k][:MAX_ITEMS]:
                                name_val = it.get("prdNm") or it.get("prodName") or it.get("goodsNm") or it.get("name")
                                brand_val = it.get("brandNm") or it.get("brand")
                                url_val = it.get("goodsUrl") or it.get("prdUrl") or it.get("url")

                                sale_val = it.get("price") or it.get("salePrice") or it.get("onlinePrice") or it.get("finalPrice")
                                org_val  = it.get("orgPrice") or it.get("originalPrice") or it.get("listPrice")
                                sale_price = parse_won_to_int(str(sale_val) if sale_val is not None else "")
                                original_price = parse_won_to_int(str(org_val) if org_val is not None else "")

                                if isinstance(url_val, str) and url_val.startswith("/"):
                                    url_val = "https://www.oliveyoung.co.kr" + url_val
                                cleaned = clean_title(name_val or "")
                                brand = brand_val or extract_brand_from_name(cleaned)

                                disc_pct = None
                                if original_price and sale_price and original_price > sale_price:
                                    disc_pct = int((original_price - sale_price) / original_price * 100)

                                out.append({
                                    "raw_name": name_val,
                                    "name": cleaned,
                                    "brand": brand,
                                    "url": url_val,
                                    "original_price": original_price,
                                    "sale_price": sale_price,
                                    "discount_pct": disc_pct,
                                    "rank": None,
                                })
                            if out:
                                logging.info("HTTP JSON parse via key %s -> %d개", k, len(out))
                                return out, text[:800]
            # HTML 파싱
            items = parse_html_products(text)
            if items:
                return items, text[:800]
        except Exception as e:
            logging.exception("HTTP candidate error: %s %s", url, e)
    return None, None

def try_playwright_render(url="https://www.oliveyoung.co.kr/store/main/getBestList.do"):
    if not PLAYWRIGHT_AVAILABLE:
        logging.warning("Playwright not available.")
        return None, None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
                locale="ko-KR"
            )
            page = context.new_page()
            logging.info("Playwright goto (try 1): https://www.oliveyoung.co.kr/store/main/getBest.do")
            try:
                page.goto("https://www.oliveyoung.co.kr/store/main/getBest.do", wait_until="domcontentloaded", timeout=60000)
            except Exception:
                pass
            logging.info("Playwright goto (try 1): https://www.oliveyoung.co.kr/store/main/getBestList.do")
            page.goto(url, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(2500)
            html = page.content()
            items = parse_html_products(html)
            browser.close()
            return items, html[:800]
    except Exception as e:
        logging.exception("Playwright render error: %s", e)
        return None, None

def fill_ranks_and_fix(items):
    out = []
    rank = 1
    for it in items:
        it["rank"] = rank
        out.append(it)
        rank += 1
        if rank > MAX_ITEMS:
            break
    return out


# ---------------- Google Drive (OAuth)
def build_drive_service_oauth():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        logging.warning("OAuth env 미설정 (GOOGLE_CLIENT_ID/SECRET/REFRESH_TOKEN)")
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
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
        return service
    except Exception as e:
        logging.exception("OAuth Drive service 생성 실패: %s", e)
        return None

def upload_csv_to_drive(service, csv_bytes, filename, folder_id=None):
    if not service:
        return None
    try:
        media = MediaIoBaseUpload(BytesIO(csv_bytes), mimetype="text/csv", resumable=False)
        body = {"name": filename}
        if folder_id:
            body["parents"] = [folder_id]
        f = service.files().create(body=body, media_body=media, fields="id,webViewLink,name").execute()
        logging.info("Uploaded to Drive: id=%s name=%s link=%s", f.get("id"), f.get("name"), f.get("webViewLink"))
        return f
    except Exception as e:
        logging.exception("Drive upload 실패: %s", e)
        return None

def find_csv_by_exact_name(service, folder_id: str, filename: str):
    """파일명(정확일치)으로 검색 (한국시간 기반 전일 파일 찾기에 사용)."""
    try:
        if folder_id:
            q = f"name='{filename}' and '{folder_id}' in parents and mimeType='text/csv'"
        else:
            q = f"name='{filename}' and mimeType='text/csv'"
        res = service.files().list(q=q, pageSize=1, fields="files(id,name,createdTime)").execute()
        files = res.get("files", [])
        return files[0] if files else None
    except Exception as e:
        logging.exception("find_csv_by_exact_name error: %s", e)
        return None

def download_file_from_drive(service, file_id):
    try:
        request = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read().decode("utf-8")
    except Exception as e:
        logging.exception("download_file_from_drive error: %s", e)
        return None


# ---------------- 분석(급상승/급하락/차트인/랭크아웃)
def analyze_trends(today_items, prev_items, top_window=TOP_WINDOW):
    """이름 기준 매칭. 전일 순위(prev_rank)와 금일 순위(rank)를 비교.
       - 급상승: prev→today 순위 개선(양수 change) 상위 정렬
       - 급하락: prev→today 순위 하락(음수 change) 하위 정렬
       - 차트인(뉴랭커): prev에 없고 today<=top_window
       - 랭크아웃: prev<=top_window 였는데 today에 없음
       - 인/아웃 카운트: 차트인 개수 + 랭크아웃 개수
    """
    prev_map = {}
    prev_top_names = set()
    for p in (prev_items or []):
        key = p.get("name") or p.get("raw_name")
        r = p.get("rank")
        prev_map[key] = r
        if r and r <= top_window:
            prev_top_names.add(key)

    trends = []
    for it in today_items:
        key = it.get("name") or it.get("raw_name")
        prev_rank = prev_map.get(key)
        if prev_rank:
            change = prev_rank - it['rank']  # +면 상승, -면 하락
            trends.append({
                "name": key,
                "brand": it.get("brand"),
                "rank": it['rank'],
                "prev_rank": prev_rank,
                "change": change,
                "sample_product": it.get("name")
            })
        else:
            # 전일 없음(뉴랭커/신규)
            trends.append({
                "name": key,
                "brand": it.get("brand"),
                "rank": it['rank'],
                "prev_rank": None,
                "change": None,
                "sample_product": it.get("name")
            })

    movers = [t for t in trends if t.get("prev_rank")]
    up_sorted = sorted(movers, key=lambda x: x["change"], reverse=True)    # 상승 많을수록 먼저
    down_sorted = sorted(movers, key=lambda x: x["change"])                # 하락 많을수록 먼저

    # 차트인: 전일에 없었고 금일 top_window 이내
    chart_ins = [t for t in trends if t["prev_rank"] is None and t["rank"] <= top_window]

    # 랭크아웃: 전일 top_window 이내였고 금일 목록에 없음
    today_names = {t.get("name") or t.get("raw_name") for t in today_items}
    rank_out_names = [nm for nm in prev_top_names if nm not in today_names]
    # prev_items에서 해당 이름과 순위 가져오기
    rank_out = []
    for p in (prev_items or []):
        nm = p.get("name") or p.get("raw_name")
        if nm in rank_out_names:
            rank_out.append({"name": nm, "brand": p.get("brand"), "prev_rank": p.get("rank")})

    in_out_count = len(chart_ins) + len(rank_out)

    return up_sorted, down_sorted, chart_ins, rank_out, in_out_count


# ---------------- Slack
def send_slack_text(text):
    if not SLACK_WEBHOOK:
        logging.warning("No SLACK_WEBHOOK configured.")
        return False
    try:
        res = requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=10)
        return res.status_code // 100 == 2
    except Exception:
        return False


# ---------------- 메인
def main():
    now_kst = kst_now()
    today_kst = now_kst.date()
    yesterday_kst = (now_kst - timedelta(days=1)).date()
    logging.info("Build: oy-app gdrive+discount %s", today_kst.isoformat())

    # 1) 스크래핑
    logging.info("Start scraping")
    items, sample = try_http_candidates()
    if not items:
        logging.info("HTTP failed → Playwright fallback")
        items, sample = try_playwright_render()
    if not items:
        logging.error("Scraping failed. sample head: %s", (sample or "")[:500])
        send_slack_text(f"❌ OliveYoung scraping failed.\n{(sample or '')[:800]}")
        return 1

    if len(items) > MAX_ITEMS:
        items = items[:MAX_ITEMS]
    items_filled = fill_ranks_and_fix(items)

    # 2) CSV 생성 (정가/할인가/할인율 포함)
    os.makedirs(OUT_DIR, exist_ok=True)
    fname_today = f"올리브영_랭킹_{today_kst.isoformat()}.csv"
    header = ["rank","brand","name","original_price","sale_price","discount_pct","url","raw_name"]
    lines = [",".join(header)]

    def q(s):
        if s is None: return ""
        s = str(s).replace('"','""')
        if any(c in s for c in [',','\n','"']): return f'"{s}"'
        return s

    for it in items_filled:
        row = [
            q(it.get("rank")),
            q(it.get("brand")),
            q(it.get("name")),
            q(it.get("original_price") if it.get("original_price") is not None else ""),
            q(it.get("sale_price") if it.get("sale_price") is not None else ""),
            q(it.get("discount_pct") if it.get("discount_pct") is not None else ""),
            q(it.get("url")),
            q(it.get("raw_name")),
        ]
        lines.append(",".join(row))
    csv_data = ("\n".join(lines)).encode("utf-8")

    # 로컬 저장
    path = os.path.join(OUT_DIR, fname_today)
    with open(path, "wb") as f:
        f.write(csv_data)
    logging.info("Saved CSV locally: %s", path)

    # 3) GDrive 업로드
    drive_service = build_drive_service_oauth()
    if drive_service and GDRIVE_FOLDER_ID:
        upload_csv_to_drive(drive_service, csv_data, fname_today, folder_id=GDRIVE_FOLDER_ID)
    else:
        logging.warning("OAuth Drive 미설정 또는 폴더ID 누락 -> 업로드 스킵")

    # 4) 전일(csv) 로드 — **한국시간 기준 파일명**으로 정확히 찾기
    prev_items = None
    if drive_service and GDRIVE_FOLDER_ID:
        fname_yesterday = f"올리브영_랭킹_{yesterday_kst.isoformat()}.csv"
        y_file = find_csv_by_exact_name(drive_service, GDRIVE_FOLDER_ID, fname_yesterday)
        # 혹시 없으면 최신(어제 업로드가 다른 시간대일 수 있으므로) 백업 전략: 최신 하나
        if not y_file:
            logging.warning("전일 파일명(%s)로 찾지 못함 → 최신 파일 백업 검색", fname_yesterday)
            try:
                q = f"mimeType='text/csv' and name contains '올리브영_랭킹' and '{GDRIVE_FOLDER_ID}' in parents"
                r = drive_service.files().list(q=q, orderBy="createdTime desc", pageSize=2,
                                               fields="files(id,name,createdTime)").execute()
                files = r.get("files", [])
                # 오늘 파일과 동일명인 것은 제외
                for fmeta in files:
                    if fmeta.get("name") != fname_today:
                        y_file = fmeta
                        break
            except Exception as e:
                logging.exception("백업 검색 실패: %s", e)

        if y_file:
            prev_csv_text = download_file_from_drive(drive_service, y_file.get("id"))
            if prev_csv_text:
                prev_items = []
                try:
                    import csv
                    sio = StringIO(prev_csv_text)
                    rdr = csv.DictReader(sio)
                    for r in rdr:
                        try:
                            prev_items.append({
                                "rank": int(r.get("rank") or 0),
                                "name": r.get("name"),
                                "raw_name": r.get("raw_name"),
                                "brand": r.get("brand"),
                            })
                        except Exception:
                            continue
                except Exception as e:
                    logging.exception("CSV parse failed: %s", e)

    # 5) 분석
    up, down, chart_ins, rank_out, in_out_count = analyze_trends(items_filled, prev_items or [], TOP_WINDOW)

    # 6) Slack 메시지 구성 — 볼드 제목/소제목, 섹션 순서 변경, 포맷 변경, 화살표/기호 조정
    # 제목
    title = f"*올리브영 데일리 전체 랭킹 100 (국내)* ({now_kst.strftime('%Y-%m-%d %H:%M KST')})"
    out_lines = [title]

    # Top10
    out_lines.append("*TOP 10*")
    top10 = items_filled[:10]
    for it in top10:
        rank = it.get("rank")
        name = it.get("name") or ""
        sale = it.get("sale_price")
        pct = it.get("discount_pct")
        price_str = fmt_price_with_discount(sale, pct)
        url = it.get("url")
        if url:
            out_lines.append(f"{rank}. <{url}|{name}> — {price_str}")
        else:
            out_lines.append(f"{rank}. {name} — {price_str}")

    # 급상승 (TOP3, 제품명 + 이동)
    def fmt_move_line(name, prev_rank, cur_rank):
        if prev_rank is None:
            return f"- {name} NEW → {cur_rank}위"
        diff = prev_rank - cur_rank
        arrow = "↑" if diff > 0 else "↓"
        return f"- {name} {prev_rank}위 → {cur_rank}위 ({arrow}{abs(diff)})"

    out_lines.append("")
    out_lines.append("*🔥 급상승*")
    if up:
        for m in up[:3]:
            # 요청대로 '브랜드명 별도라인' 제거 → 상품명 라인만
            name = m.get("sample_product") or m.get("name")
            out_lines.append(fmt_move_line(name, m.get("prev_rank"), m.get("rank")))
    else:
        out_lines.append("- (이전 데이터 없음)")

    # 뉴랭커(차트인)
    out_lines.append("")
    out_lines.append("*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:3]:
            # 차트인 포맷: "이름 NEW → N위" + 보조 기호는 ↳(선호)
            out_lines.append(f"- {t.get('name')} NEW → {t.get('rank')}위")
    else:
        out_lines.append("- (변동 없음)")

    # 급하락 (TOP5) + 랭크아웃 표기
    out_lines.append("")
    out_lines.append("*📉 급하락*")
    showed = 0
    for m in down:
        if showed >= 5:
            break
        diff = m["rank"] - m["prev_rank"]
        if diff > 0:  # 하락
            name = m.get("sample_product") or m.get("name")
            out_lines.append(f"- {name} {m['prev_rank']}위 → {m['rank']}위 (↓{diff})")
            showed += 1
    # 랭크아웃
    if rank_out:
        for ro in rank_out:
            out_lines.append(f"- {ro.get('name')} {ro.get('prev_rank')}위 → OUT")

    # 랭크 인&아웃 개수
    out_lines.append("")
    out_lines.append("*↔ 랭크 인&아웃*")
    out_lines.append(f"{in_out_count}개의 제품이 인&아웃 되었습니다.")

    # 전송
    send_slack_text("\n".join(out_lines))
    logging.info("Done.")
    return 0


if __name__ == "__main__":
    exit(main())
