#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# brand_rank_app.py — 올리브영 모바일 브랜드 랭킹 크롤링 + GDrive 업로드 + Slack 리포트

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

# (optional) Playwright fallback (필요 시 사용)
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
MAX_ITEMS = 100        # 크롤링 최대 아이템 (브랜드 순위)
TOP_WINDOW = 30        # 인/아웃 판정 기준 윈도우

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


# ---------------- 유틸 (기존 코드와 동일)
def kst_now():
    return datetime.now(timezone.utc) + timedelta(hours=9)

def make_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        # 모바일 User-Agent로 변경
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://m.oliveyoung.co.kr/m/mtn?menu=ranking&tab=brands",
    })
    return s

# ---------------- 파싱/정제 (브랜드 랭킹에 맞게 수정)
def parse_brand_html(html: str):
    """모바일 브랜드 랭킹 HTML에서 브랜드 정보를 파싱합니다."""
    soup = BeautifulSoup(html, "html.parser")
    # 브랜드 랭킹 리스트의 CSS Selector
    list_items = soup.select("div.rank_brand_list > ul > li")
    
    results = []
    if not list_items:
        logging.warning("브랜드 랭킹 리스트(.rank_brand_list > ul > li)를 찾을 수 없습니다.")
        return []

    for item in list_items[:MAX_ITEMS]:
        # 순위
        rank_node = item.select_one(".rank_num")
        rank = int(rank_node.get_text(strip=True)) if rank_node else None

        # 브랜드명
        brand_name_node = item.select_one(".brand_name")
        brand_name = brand_name_node.get_text(strip=True) if brand_name_node else ""
        
        # 브랜드 링크
        link_node = item.select_one("a.brand_item")
        href = link_node.get("href") if link_node else ""
        if href and not href.startswith("http"):
             href = "https://m.oliveyoung.co.kr" + href

        # 대표 상품명 (여러 개 중 첫 번째 것만 가져옴)
        product_name_node = item.select_one(".prd_name")
        product_name = product_name_node.get_text(strip=True) if product_name_node else ""

        if rank and brand_name:
            results.append({
                "rank": rank,
                "brand_name": brand_name,
                "representative_product": product_name,
                "url": href
            })
    
    logging.info("parse_brand_html: %d개의 브랜드 순위를 파싱했습니다.", len(results))
    return results

def fetch_brand_ranking_data():
    """올리브영 모바일 브랜드 랭킹 페이지 데이터를 가져옵니다."""
    session = make_session()
    # 모바일 브랜드 랭킹 URL
    url = "https://m.oliveyoung.co.kr/m/mtn/ranking/getBrandRanking.do"
    
    try:
        logging.info("HTTP GET: %s", url)
        # 이 페이지는 별도 파라미터 없이 GET 요청으로 HTML을 반환
        r = session.get(url, timeout=20)
        logging.info(" -> status=%s, ct=%s, len=%d", r.status_code, r.headers.get("Content-Type"), len(r.text or ""))
        
        if r.status_code == 200 and "text/html" in r.headers.get("Content-Type", ""):
            items = parse_brand_html(r.text)
            return items, r.text[:800]
        else:
            # 실패 시 Cloudflare 페이지일 가능성 있음
            return None, r.text[:800]
            
    except Exception as e:
        logging.exception("HTTP 요청 실패: %s", e)
        return None, str(e)

# ---------------- Google Drive (기존 코드와 거의 동일, 파일명만 변경)
def build_drive_service_oauth():
    # ... (기존 코드와 동일) ...
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
    # ... (기존 코드와 동일) ...
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
    # ... (기존 코드와 동일) ...
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
    # ... (기존 코드와 동일) ...
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


# ---------------- 분석 (브랜드 순위에 맞게 수정)
def analyze_brand_trends(today_items, prev_items, top_window=TOP_WINDOW):
    """브랜드명 기준 매칭. 전일 순위(prev_rank)와 금일 순위(rank)를 비교."""
    prev_map = {p.get("brand_name"): p.get("rank") for p in (prev_items or [])}
    prev_top_brands = {p.get("brand_name") for p in (prev_items or []) if p.get("rank") and p.get("rank") <= top_window}

    trends = []
    for it in today_items:
        brand_name = it["brand_name"]
        prev_rank = prev_map.get(brand_name)
        trends.append({
            "brand_name": brand_name,
            "rank": it['rank'],
            "prev_rank": prev_rank,
            "change": prev_rank - it['rank'] if prev_rank else None,
        })
    
    movers = [t for t in trends if t.get("prev_rank")]
    up_sorted = sorted(movers, key=lambda x: x["change"], reverse=True)
    down_sorted = sorted(movers, key=lambda x: x["change"])

    chart_ins = [t for t in trends if t["prev_rank"] is None and t["rank"] <= top_window]
    
    today_brands = {t["brand_name"] for t in today_items}
    rank_out_brands = [nm for nm in prev_top_brands if nm not in today_brands]
    
    rank_out = []
    for p in (prev_items or []):
        if p.get("brand_name") in rank_out_brands:
            rank_out.append(p)

    in_out_count = len(chart_ins) + len(rank_out)
    return up_sorted, down_sorted, chart_ins, rank_out, in_out_count

# ---------------- Slack (기존 코드와 동일)
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
    logging.info("Build: oy-brand-rank-app %s", today_kst.isoformat())

    # 1) 스크래핑
    logging.info("Start scraping brand ranking")
    items, sample = fetch_brand_ranking_data()
    
    if not items:
        logging.error("Scraping failed. sample head: %s", (sample or "")[:500])
        send_slack_text(f"❌ OliveYoung Mobile Brand Ranking scraping failed.\n{(sample or '')[:800]}")
        return 1

    # 2) CSV 생성
    os.makedirs(OUT_DIR, exist_ok=True)
    fname_today = f"올리브영_브랜드랭킹_{today_kst.isoformat()}.csv"
    header = ["rank", "brand_name", "representative_product", "url"]
    
    def q(s):
        if s is None: return ""
        s = str(s).replace('"', '""')
        if any(c in s for c in [',', '\n', '"']): return f'"{s}"'
        return s

    lines = [",".join(header)]
    for it in items:
        lines.append(",".join([q(it.get(h)) for h in header]))
    
    csv_data = "\n".join(lines).encode("utf-8")

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

    # 4) 전일 데이터 로드
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
                        try:
                            r['rank'] = int(r.get('rank', 0))
                            prev_items.append(r)
                        except (ValueError, TypeError):
                            continue
                except Exception as e:
                    logging.exception("Previous CSV parse failed: %s", e)
    
    # 5) 분석
    up, down, chart_ins, rank_out, in_out_count = analyze_brand_trends(items, prev_items or [], TOP_WINDOW)

    # 6) Slack 메시지 구성
    title = f"*올리브영 모바일 브랜드 랭킹 100* ({now_kst.strftime('%Y-%m-%d %H:%M KST')})"
    out_lines = [title]

    # Top10
    out_lines.append("\n*🏆 TOP 10 브랜드*")
    for it in items[:10]:
        rank = it.get("rank")
        brand = it.get("brand_name")
        url = it.get("url")
        out_lines.append(f"{rank}. <{url}|{brand}>")

    # 급상승
    def fmt_brand_move(brand, prev, cur):
        diff = prev - cur
        arrow = "↑" if diff > 0 else "↓"
        return f"- {brand} {prev}위 → {cur}위 ({arrow}{abs(diff)})"

    out_lines.append("\n*🔥 급상승*")
    for m in up[:3]:
        out_lines.append(fmt_brand_move(m["brand_name"], m["prev_rank"], m["rank"]))

    # 뉴랭커(차트인)
    out_lines.append("\n*🆕 뉴랭커*")
    for t in chart_ins[:3]:
        out_lines.append(f"- {t['brand_name']} NEW → {t['rank']}위")

    # 급하락 & 랭크아웃
    out_lines.append("\n*📉 급하락 & 랭크아웃*")
    for m in down[:3]:
        out_lines.append(fmt_brand_move(m["brand_name"], m["prev_rank"], m["rank"]))
    for ro in rank_out[:2]:
        out_lines.append(f"- {ro['brand_name']} {ro['rank']}위 → OUT")

    # 인&아웃
    out_lines.append(f"\n*↔ 랭크 인&아웃*: {in_out_count}개 브랜드 변동")

    send_slack_text("\n".join(out_lines))
    logging.info("Done.")
    return 0

if __name__ == "__main__":
    exit(main())
