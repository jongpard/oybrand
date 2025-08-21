#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# brand_rank_app.py — 올리브영 모바일 브랜드 랭킹 크롤링 (Playwright Stealth 적용 최종본)

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

# Playwright & Stealth
try:
    from playwright.sync_api import sync_playwright
    from playwright_stealth import stealth_sync # ✨ Stealth 라이브러리 import
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
MAX_ITEMS = 100
TOP_WINDOW = 30
SCREENSHOT_PATH = "debug_screenshot.png"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# ... (유틸, 파싱 함수는 이전과 동일) ...
def kst_now():
    return datetime.now(timezone.utc) + timedelta(hours=9)

def make_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://m.oliveyoung.co.kr/m/main.do",
    })
    return s

def parse_brand_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
    list_items = soup.select("div.rank_brand_list > ul > li")
    results = []
    if not list_items: return []
    for item in list_items[:MAX_ITEMS]:
        rank_node = item.select_one(".rank_num"); rank = int(rank_node.get_text(strip=True)) if rank_node else None
        brand_name_node = item.select_one(".brand_name"); brand_name = brand_name_node.get_text(strip=True) if brand_name_node else ""
        link_node = item.select_one("a.brand_item"); href = link_node.get("href") if link_node else ""
        if href and not href.startswith("http"): href = "https://m.oliveyoung.co.kr" + href
        product_name_node = item.select_one(".prd_name"); product_name = product_name_node.get_text(strip=True) if product_name_node else ""
        if rank and brand_name:
            results.append({"rank": rank, "brand_name": brand_name, "representative_product": product_name, "url": href})
    logging.info("parse_brand_html: %d개의 브랜드 순위를 파싱했습니다.", len(results))
    return results

def fetch_brand_ranking_data(): # 이 함수는 이제 거의 항상 실패하지만, 만일을 위해 남겨둡니다.
    session = make_session()
    url = "https://m.oliveyoung.co.kr/m/mtn/ranking/getBrandRanking.do"
    try:
        r = session.get(url, timeout=15)
        if r.status_code != 200: return None, r.text[:500]
        items = parse_brand_html(r.text)
        return (items, r.text[:500]) if items else (None, r.text[:500])
    except Exception:
        return None, "HTTP Request Exception"

# --- ✨ Stealth 모드가 적용된 Playwright 함수 ---
def try_playwright_for_brands_stealth():
    if not PLAYWRIGHT_AVAILABLE:
        logging.warning("Playwright not available.")
        return None, None
    
    ranking_url = "https://m.oliveyoung.co.kr/m/mtn/ranking/getBrandRanking.do"
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = browser.new_context(
                user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17_5 Mobile/15E148 Safari/604.1",
                locale="ko-KR",
                viewport={'width': 390, 'height': 844}
            )
            page = context.new_page()
            
            # ✨ Stealth 모드 적용 ✨
            stealth_sync(page)

            logging.info("Playwright (Stealth): Navigating to brand ranking page.")
            # 페이지 로드가 완료될 때까지 최대 60초 대기
            page.goto(ranking_url, wait_until="networkidle", timeout=60000)
            
            try:
                # 랭킹 리스트가 나타날 때까지 최대 30초 대기
                page.wait_for_selector("div.rank_brand_list > ul > li", timeout=30000)
                logging.info("Playwright (Stealth): Ranking list element found successfully.")
            except Exception as e:
                page.screenshot(path=SCREENSHOT_PATH)
                logging.error("Playwright (Stealth): Timeout waiting for selector. Debug screenshot saved.")
                raise e

            html = page.content()
            items = parse_brand_html(html)
            browser.close()
            return items, html[:500]
            
    except Exception as e:
        logging.exception("Playwright (Stealth) render error: %s", e)
        return None, f"Playwright (Stealth) failed. Check screenshot. Error: {str(e)}"

# ... (Google Drive, 분석, Slack 함수는 이전과 동일하게 유지) ...
def build_drive_service_oauth():
    # ...
def upload_csv_to_drive(service, csv_bytes, filename, folder_id=None):
    # ...
def find_csv_by_exact_name(service, folder_id: str, filename: str):
    # ...
def download_file_from_drive(service, file_id):
    # ...
def analyze_brand_trends(today_items, prev_items, top_window=TOP_WINDOW):
    # ...
def send_slack_text(text):
    # ...

# ---------------- 메인
def main():
    now_kst = kst_now()
    today_kst = now_kst.date()
    yesterday_kst = (now_kst - timedelta(days=1)).date()
    logging.info("Build: oy-brand-rank-app %s", today_kst.isoformat())

    # --- ✨ 수정된 크롤링 로직: 이제 Playwright를 우선적으로 사용 ---
    logging.info("Start scraping brand ranking (Playwright Stealth First)")
    items, sample = try_playwright_for_brands_stealth()

    if not items:
        # 만약을 위한 HTTP 재시도
        logging.warning("Playwright failed, trying HTTP request as a last resort.")
        items, sample = fetch_brand_ranking_data()

    if not items:
        logging.error("Scraping failed completely. sample head: %s", (sample or "")[:500])
        send_slack_text(f"❌ OliveYoung Mobile Brand Ranking scraping failed.\nLast message: {(sample or '')[:800]}")
        return 1
    
    # ... (CSV 생성 및 이후 로직은 이전과 동일) ...
    os.makedirs(OUT_DIR, exist_ok=True)
    fname_today = f"올리브영_브랜드랭킹_{today_kst.isoformat()}.csv"
    header = ["rank", "brand_name", "representative_product", "url"]
    def q(s):
        if s is None: return ""
        s = str(s).replace('"', '""'); return f'"{s}"' if any(c in s for c in [',', '\n', '"']) else s
    lines = [",".join(header)]
    for it in items: lines.append(",".join([q(it.get(h)) for h in header]))
    csv_data = "\n".join(lines).encode("utf-8")
    path = os.path.join(OUT_DIR, fname_today)
    with open(path, "wb") as f: f.write(csv_data)
    logging.info("Saved CSV locally: %s", path)

    drive_service = build_drive_service_oauth()
    if drive_service and GDRIVE_FOLDER_ID:
        upload_csv_to_drive(drive_service, csv_data, fname_today, folder_id=GDRIVE_FOLDER_ID)
    
    prev_items = None
    if drive_service and GDRIVE_FOLDER_ID:
        fname_yesterday = f"올리브영_브랜드랭킹_{yesterday_kst.isoformat()}.csv"
        y_file = find_csv_by_exact_name(drive_service, GDRIVE_FOLDER_ID, fname_yesterday)
        if y_file:
            prev_csv_text = download_file_from_drive(drive_service, y_file.get("id"))
            if prev_csv_text:
                prev_items = []
                try:
                    import csv
                    sio = StringIO(prev_csv_text)
                    rdr = csv.DictReader(sio)
                    for r in rdr:
                        try: r['rank'] = int(r.get('rank', 0)); prev_items.append(r)
                        except (ValueError, TypeError): continue
                except Exception: pass
    
    up, down, chart_ins, rank_out, in_out_count = analyze_brand_trends(items, prev_items or [], TOP_WINDOW)

    title = f"*올리브영 모바일 브랜드 랭킹 100* ({now_kst.strftime('%Y-%m-%d %H:%M KST')})"
    out_lines = [title]
    out_lines.append("\n*🏆 TOP 10 브랜드*")
    for it in items[:10]: out_lines.append(f"{it.get('rank')}. <{it.get('url')}|{it.get('brand_name')}>")
    def fmt_brand_move(brand, prev, cur): return f"- {brand} {prev}위 → {cur}위 ({'↑' if prev > cur else '↓'}{abs(prev - cur)})"
    out_lines.append("\n*🔥 급상승*")
    for m in up[:3]: out_lines.append(fmt_brand_move(m["brand_name"], m["prev_rank"], m["rank"]))
    out_lines.append("\n*🆕 뉴랭커*")
    for t in chart_ins[:3]: out_lines.append(f"- {t['brand_name']} NEW → {t['rank']}위")
    out_lines.append("\n*📉 급하락 & 랭크아웃*")
    for m in down[:3]: out_lines.append(fmt_brand_move(m["brand_name"], m["prev_rank"], m["rank"]))
    for ro in rank_out[:2]: out_lines.append(f"- {ro['brand_name']} {ro['rank']}위 → OUT")
    out_lines.append(f"\n*↔ 랭크 인&아웃*: {in_out_count}개 브랜드 변동")

    send_slack_text("\n".join(out_lines))
    logging.info("Done.")
    return 0

if __name__ == "__main__":
    exit(main())
