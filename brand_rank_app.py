#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# brand_rank_app.py — 올리브영 모바일 브랜드 랭킹 크롤링 (최종 안정화)

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

# Playwright & Stealth - 이제 필수 라이브러리이므로 예외 처리 없이 바로 import합니다.
# 만약 설치에 문제가 있었다면 여기서 바로 ImportError가 발생하여 원인을 알 수 있습니다.
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

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

# ---------------- 유틸, 파싱 (이전과 동일)
def kst_now():
    return datetime.now(timezone.utc) + timedelta(hours=9)

def parse_brand_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
    list_items = soup.select("div.rank_brand_list > ul > li")
    results = []
    if not list_items:
        logging.warning("HTML에서 브랜드 랭킹 리스트를 찾지 못했습니다.")
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

# --- Stealth 모드가 적용된 Playwright 함수 ---
def crawl_with_playwright():
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
            stealth_sync(page)

            logging.info("Playwright (Stealth): Navigating to brand ranking page.")
            page.goto(ranking_url, wait_until="networkidle", timeout=60000)
            
            try:
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

# ---------------- Google Drive, 분석, Slack 함수 (이전과 동일)
def build_drive_service_oauth():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN): return None
    try:
        creds = UserCredentials(None, refresh_token=GOOGLE_REFRESH_TOKEN, client_id=GOOGLE_CLIENT_ID, client_secret=GOOGLE_CLIENT_SECRET, token_uri="https://oauth2.googleapis.com/token", scopes=["https://www.googleapis.com/auth/drive.file"])
        creds.refresh(GoogleRequest()); return build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception as e: logging.exception("OAuth Drive service 생성 실패: %s", e); return None

def upload_csv_to_drive(service, csv_bytes, filename, folder_id=None):
    if not service: return None
    try:
        media = MediaIoBaseUpload(BytesIO(csv_bytes), mimetype="text/csv", resumable=False); body = {"name": filename}
        if folder_id: body["parents"] = [folder_id]
        f = service.files().create(body=body, media_body=media, fields="id,webViewLink,name").execute()
        logging.info("Uploaded to Drive: id=%s name=%s link=%s", f.get("id"), f.get("name"), f.get("webViewLink")); return f
    except Exception as e: logging.exception("Drive upload 실패: %s", e); return None

def find_csv_by_exact_name(service, folder_id: str, filename: str):
    try:
        q = f"name='{filename}' and '{folder_id}' in parents and mimeType='text/csv'"
        res = service.files().list(q=q, pageSize=1, fields="files(id,name,createdTime)").execute()
        return res.get("files", [])[0] if res.get("files") else None
    except Exception as e: logging.exception("find_csv_by_exact_name error: %s", e); return None
        
def download_file_from_drive(service, file_id):
    try:
        request = service.files().get_media(fileId=file_id); fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request); done = False
        while not done: status, done = downloader.next_chunk()
        fh.seek(0); return fh.read().decode("utf-8")
    except Exception as e: logging.exception("download_file_from_drive error: %s", e); return None

def analyze_brand_trends(today_items, prev_items, top_window=TOP_WINDOW):
    prev_map = {p.get("brand_name"): p.get("rank") for p in (prev_items or [])}
    prev_top_brands = {p.get("brand_name") for p in (prev_items or []) if p.get("rank") and p.get("rank") <= top_window}
    trends = []; up, down, ins, out = [], [], [], []
    for it in today_items:
        brand = it["brand_name"]; prev_rank = prev_map.get(brand)
        if prev_rank: movers.append({"brand_name": brand, "rank": it['rank'], "prev_rank": prev_rank, "change": prev_rank - it['rank']})
        elif it['rank'] <= top_window: ins.append({"brand_name": brand, "rank": it['rank']})
    today_brands = {t["brand_name"] for t in today_items}
    out_brands = [p for p in (prev_items or []) if p.get("brand_name") in (prev_top_brands - today_brands)]
    up = sorted([m for m in movers if m["change"] > 0], key=lambda x: x["change"], reverse=True)
    down = sorted([m for m in movers if m["change"] < 0], key=lambda x: x["change"])
    return up, down, ins, out, len(ins) + len(out)

def send_slack_text(text):
    if not SLACK_WEBHOOK: logging.warning("No SLACK_WEBHOOK configured."); return False
    try: res = requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=10); return res.ok
    except Exception: return False

# ---------------- 메인
def main():
    now_kst = kst_now()
    today_kst = now_kst.date()
    yesterday_kst = (now_kst - timedelta(days=1)).date()
    logging.info("Build: oy-brand-rank-app %s", today_kst.isoformat())

    # --- 크롤링 로직: Playwright-Stealth를 기본으로 사용 ---
    items, sample = crawl_with_playwright()

    if not items:
        logging.error("Scraping failed completely. sample head: %s", (sample or "")[:500])
        send_slack_text(f"❌ OliveYoung Mobile Brand Ranking scraping failed.\nLast message: {(sample or '')[:800]}")
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
    with open(os.path.join(OUT_DIR, fname_today), "wb") as f: f.write(csv_data)
    logging.info("Saved CSV locally.")

    drive_service = build_drive_service_oauth()
    if drive_service and GDRIVE_FOLDER_ID:
        upload_csv_to_drive(drive_service, csv_data, fname_today, folder_id=GDRIVE_FOLDER_ID)
    
    # --- 전일 데이터 로드 및 분석 ---
    prev_items = []
    if drive_service and GDRIVE_FOLDER_ID:
        fname_yesterday = f"올리브영_브랜드랭킹_{yesterday_kst.isoformat()}.csv"
        y_file = find_csv_by_exact_name(drive_service, GDRIVE_FOLDER_ID, fname_yesterday)
        if y_file:
            prev_csv_text = download_file_from_drive(drive_service, y_file.get("id"))
            if prev_csv_text:
                try:
                    import csv
                    sio = StringIO(prev_csv_text)
                    rdr = csv.DictReader(sio)
                    for r in rdr:
                        try: r['rank'] = int(r.get('rank', 0)); prev_items.append(r)
                        except (ValueError, TypeError): continue
                except Exception: pass
    
    up, down, chart_ins, rank_out, in_out_count = analyze_brand_trends(items, prev_items, TOP_WINDOW)

    # --- Slack 메시지 구성 ---
    title = f"*올리브영 모바일 브랜드 랭킹 100* ({now_kst.strftime('%Y-%m-%d %H:%M KST')})"
    out_lines = [title]
    out_lines.append("\n*🏆 TOP 10 브랜드*")
    for it in items[:10]: out_lines.append(f"{it.get('rank')}. <{it.get('url')}|{it.get('brand_name')}>")
    def fmt_brand_move(brand, prev, cur): return f"- {brand} {prev}위 → {cur}위 ({'↑' if prev > cur else '↓'}{abs(prev - cur)})"
    out_lines.append("\n*🔥 급상승*"); [out_lines.append(fmt_brand_move(m["brand_name"], m["prev_rank"], m["rank"])) for m in up[:3]]
    out_lines.append("\n*🆕 뉴랭커*"); [out_lines.append(f"- {t['brand_name']} NEW → {t['rank']}위") for t in chart_ins[:3]]
    out_lines.append("\n*📉 급하락 & 랭크아웃*"); [out_lines.append(fmt_brand_move(m["brand_name"], m["prev_rank"], m["rank"])) for m in down[:3]]
    [out_lines.append(f"- {ro['brand_name']} {ro['rank']}위 → OUT") for ro in rank_out[:2]]
    out_lines.append(f"\n*↔ 랭크 인&아웃*: {in_out_count}개 브랜드 변동")

    send_slack_text("\n".join(out_lines))
    logging.info("Done.")
    return 0

if __name__ == "__main__":
    exit(main())
