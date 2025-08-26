#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dist/weekly_report.html 을 Google Drive 지정 폴더로 업로드.
동일 이름 존재시 업데이트, 없으면 생성.
"""

import os
import sys
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

DIST_DIR = os.environ.get("DIST_DIR", "dist")
HTML_FILE = os.path.join(DIST_DIR, "weekly_report.html")

FOLDER_ID      = os.environ.get("GDRIVE_FOLDER_ID", "")
CLIENT_ID      = os.environ.get("GOOGLE_CLIENT_ID", "")
CLIENT_SECRET  = os.environ.get("GOOGLE_CLIENT_SECRET", "")
REFRESH_TOKEN  = os.environ.get("GOOGLE_REFRESH_TOKEN", "")

def get_service():
    if not (FOLDER_ID and CLIENT_ID and CLIENT_SECRET and REFRESH_TOKEN):
        print("[GDRIVE] 환경변수 누락: GDRIVE_FOLDER_ID/GOOGLE_*", file=sys.stderr)
        sys.exit(0)

    creds = Credentials(
        None,
        refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def find_existing(service, folder_id, name):
    # f-string 내부 표현식에 백슬래시가 들어가지 않도록 선처리
    safe = name.replace("'", "\\'")
    q = f"name = '{safe}' and '{folder_id}' in parents and trashed = false"
    resp = service.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=10).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None

def upload(service, folder_id, path, name):
    file_id = find_existing(service, folder_id, name)
    media = MediaFileUpload(path, mimetype="text/html", resumable=False)
    if file_id:
        print(f"[GDRIVE] update: {name}")
        service.files().update(fileId=file_id, media_body=media).execute()
    else:
        print(f"[GDRIVE] create: {name}")
        metadata = {"name": name, "parents": [folder_id], "mimeType": "text/html"}
        service.files().create(body=metadata, media_body=media, fields="id").execute()

def main():
    if not os.path.exists(HTML_FILE):
        print(f"[GDRIVE] 업로드할 HTML이 없음: {HTML_FILE}", file=sys.stderr)
        sys.exit(0)

    name = os.path.basename(HTML_FILE)
    try:
        svc = get_service()
        upload(svc, FOLDER_ID, HTML_FILE, name)
        print("[GDRIVE] done")
    except HttpError as e:
        print(f"[GDRIVE] API error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
