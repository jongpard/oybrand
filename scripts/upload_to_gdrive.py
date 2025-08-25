# -*- coding: utf-8 -*-
"""
필요 시에만 사용. (우선 Slack/HTML 안정화 후 on)
환경변수:
- GDRIVE_FOLDER_ID
- GOOGLE_CLIENT_ID
- GOOGLE_CLIENT_SECRET
- GOOGLE_REFRESH_TOKEN
"""
import os
import json
import time
import requests
from pathlib import Path

TOKEN_URL = "https://oauth2.googleapis.com/token"
UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart"
SEARCH_URL = "https://www.googleapis.com/drive/v3/files"

def _token():
    data = {
        "client_id": os.environ["GOOGLE_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_REFRESH_TOKEN"],
        "grant_type": "refresh_token",
    }
    r = requests.post(TOKEN_URL, data=data, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def _escape_single_quotes(s: str) -> str:
    # f-string 안에서 백슬래시 연산 금지 → 미리 계산
    return s.replace("'", r"\'")

def _search_file(session, name, folder_id):
    # 이름+폴더로 검색
    q_name = _escape_single_quotes(name)
    q = f"name = '{q_name}' and '{folder_id}' in parents and trashed = false"
    r = session.get(SEARCH_URL, params={"q": q, "fields": "files(id,name)"}, timeout=30)
    r.raise_for_status()
    files = r.json().get("files", [])
    return files[0]["id"] if files else None

def upload(html_path: str, folder_id: str):
    access = _token()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access}"})

    name = Path(html_path).name
    fid = _search_file(session, name, folder_id)

    meta = {"name": name, "parents": [folder_id]}
    files = {
        "metadata": ("metadata", json.dumps(meta), "application/json; charset=UTF-8"),
        "file": (name, open(html_path, "rb"), "text/html"),
    }
    if fid:
        url = f"https://www.googleapis.com/upload/drive/v3/files/{fid}?uploadType=multipart"
        r = session.patch(url, files=files, timeout=60)
    else:
        r = session.post(UPLOAD_URL, files=files, timeout=60)
    r.raise_for_status()
    print("GDRIVE: uploaded", name)

if __name__ == "__main__":
    dest = os.environ.get("GDRIVE_FOLDER_ID")
    assert dest, "GDRIVE_FOLDER_ID is empty"
    html = "dist/weekly_report.html"
    assert Path(html).exists(), "dist/weekly_report.html not found"
    upload(html, dest)
