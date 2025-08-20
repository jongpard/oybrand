import asyncio
import os
import re
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from calendar import monthrange
from collections import OrderedDict

import json
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
URL = "https://m.oliveyoung.co.kr/m/mtn?menu=ranking&tab=brands"

OUTPUT_DIR = "data"
XLSX_NAME = "올리브영_브랜드_순위.xlsx"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, XLSX_NAME)

# 환경변수(Secrets)
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID   = os.environ.get("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID   = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "")

# -------------------------
# Playwright (모바일) 크롤링
# -------------------------
async def close_banners(page):
    # 보이는 팝업/앱유도 닫기 시도 (있어도 없어도 통과)
    candidates = [
        "text=닫기", "text=취소", "text=나중에", "role=button[name='닫기']",
        "button[aria-label*='닫기']", "[class*='btn_close']"
    ]
    for sel in candidates:
        try:
            el = await page.query_selector(sel)
            if el:
                await el.click(timeout=1000)
        except Ex
