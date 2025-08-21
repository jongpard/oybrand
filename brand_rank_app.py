#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# brand_rank_app.py — 올리브영 모바일 브랜드 랭킹 크롤링 (Playwright 안정성 강화)

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

# Playwright fallback
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
MAX_ITEMS = 100
TOP_WINDOW = 30
SCREENSHOT_PATH = "debug_screenshot.png" # 에러 발생 시 스크린샷 저장 경로

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# ---------------- 유틸 (이전과 동일)
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

# ---------------- 파싱/정제 (이전과 동일)
def parse_brand_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
    list_items = soup.select("div.rank_brand_list > ul > li")
    
    results = []
    if not list_items:
        logging.warning("브랜드 랭킹 리스트(.rank_brand_list > ul > li)를 찾을 수 없습니다.")
        return []

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

def fetch_brand_ranking_data():
    session = make_session()
    url = "https://m.oliveyoung.co.kr/m/mtn/ranking/getBrandRanking.do"
    try:
        logging.info("HTTP GET: %s", url)
        r = session.get(url, timeout=20)
        logging.info(" -> status=%s, ct=%s, len=%d", r.status_code, r.headers.get("Content-Type"), len(r.text or ""))
        if r.status_code != 200: return None, r.text[:800]
        items = parse_brand_html(r.text)
        return (items, r.text[:800]) if items else (None, r.text[:800])
    except Exception as e:
        logging.exception("HTTP 요청 실패: %s", e)
        return None, str(e)

# --- ✨ 안정성이 강화된 Playwright Fallback 함수 ---
def try_playwright_for_brands():
    if not PLAYWRIGHT_AVAILABLE:
        logging.warning("Playwright not available.")
        return None, None
    
    main_url = "https://m.oliveyoung.co.kr/m/main.do"
    ranking_url = "https://m.oliveyoung.co.kr/m/mtn/ranking/getBrandRanking.do"
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
            context = browser.new_context(
                user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
                locale="ko-KR",
                viewport={'width': 390, 'height': 844}
            )
            page = context.new_page()
            
            # 1. 메인 페이지를 먼저 방문하여 세션 활성화
            logging.info("Playwright: Visiting main page first to get session cookies.")
            page.goto(main_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(2000) # 팝업 등 로딩 대기
            
            # 2. 브랜드 랭킹 페이지로 이동
            logging.info("Playwright: Navigating to brand ranking page.")
            page.goto(ranking_url, wait_until="networkidle", timeout=40000)
            
            # 3. 랭킹 리스트가 나타날 때까지 최대 20초 대기
            try:
                page.wait_for_selector("div.rank_brand_list > ul > li", timeout=20000)
                logging.info("Playwright: Ranking list element found.")
            except Exception as e:
                # 타임아웃 발생 시 스크린샷 저장
                page.screenshot(path=SCREENSHOT_PATH)
                logging.error("Playwright: Timeout waiting for selector. Debug screenshot saved to %s", SCREENSHOT_PATH)
                raise e # 에러를 다시 발생시켜 catch 블록으로 넘김

            html = page.content()
            items = parse_brand_html(html)
            browser.close()
            return items, html[:800]
            
    except Exception as e:
        logging.exception("Playwright render error: %s", e)
        # 스크린샷 파일이 존재하면 아티팩트로 업로드할 수 있도록 경로 반환
        return None, f"Playwright failed. Check screenshot artifact if available. Error: {str(e)}"

# ... (Google Drive, 분석, Slack 함수는 이전과 동일하게 유지) ...
# (이하 생략 - 이전 답변의 Google Drive, 분석, Slack, 메인 함수 부분을 그대로 사용하시면 됩니다)

# ---------------- 메인
def main():
    now_kst = kst_now()
    today_kst = now_kst.date()
    yesterday_kst = (now_kst - timedelta(days=1)).date()
    logging.info("Build: oy-brand-rank-app %s", today_kst.isoformat())

    # --- 크롤링 로직 (Playwright Fallback) ---
    logging.info("Start scraping brand ranking (HTTP First)")
    items, sample = fetch_brand_ranking_data()
    
    if not items:
        logging.warning("HTTP request failed, falling back to Playwright.")
        items, sample = try_playwright_for_brands()

    if not items:
        logging.error("Scraping failed completely. sample head: %s", (sample or "")[:500])
        send_slack_text(f"❌ OliveYoung Mobile Brand Ranking scraping failed.\n{(sample or '')[:800]}")
        return 1
    
    # --- CSV 생성 및 업로드 ---
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
    else:
        logging.warning("OAuth Drive 미설정 또는 폴더ID 누락 -> 업로드 스킵")

    # --- 전일 데이터 로드 및 분석 ---
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
                except Exception as e: logging.exception("Previous CSV parse failed: %s", e)
    
    up, down, chart_ins, rank_out, in_out_count = analyze_brand_trends(items, prev_items or [], TOP_WINDOW)

    # --- Slack 메시지 구성 ---
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
    exit(main())```

### **워크플로우 파일 수정 (`brand_crawler.yml`)**

에러 발생 시 스크린샷을 아티팩트로 저장하여 원인을 쉽게 파악할 수 있도록 워크플로우 파일에 한 단계를 추가합니다.

```yaml
# .github/workflows/brand_crawler.yml

name: 올리브영 브랜드 랭킹 크롤러

on:
  workflow_dispatch:
  schedule:
    - cron: '00 16 * * *'

jobs:
  build-and-run:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests beautifulsoup4 google-api-python-client google-auth-httplib2 google-auth-oauthlib playwright

      - name: Install Playwright Browsers
        run: playwright install --with-deps chromium

      - name: Run Brand Ranking Crawler
        env:
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
          GOOGLE_CLIENT_ID: ${{ secrets.GOOGLE_CLIENT_ID }}
          GOOGLE_CLIENT_SECRET: ${{ secrets.GOOGLE_CLIENT_SECRET }}
          GOOGLE_REFRESH_TOKEN: ${{ secrets.GOOGLE_REFRESH_TOKEN }}
          GDRIVE_FOLDER_ID: ${{ secrets.GDRIVE_FOLDER_ID }}
        run: python brand_rank_app.py

      # --- ✨ 수정/추가된 부분 ---
      - name: Upload debug screenshot on failure
        # 스크립트가 실패했을 때만 이 단계를 실행
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: debug-screenshot
          path: debug_screenshot.png # 스크립트가 생성한 스크린샷 파일

      - name: Upload ranking data artifact
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: brand-ranking-csv
          path: rankings/
