#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
import time
from typing import Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# 환경변수에서 OAuth2 정보 읽음
ENV_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
ENV_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
ENV_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

def make_drive_service():
    if not (ENV_CLIENT_ID and ENV_CLIENT_SECRET and ENV_REFRESH_TOKEN):
        raise RuntimeError("GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / GOOGLE_REFRESH_TOKEN 누락")

    creds = Credentials(
        None,
        refresh_token=ENV_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=ENV_CLIENT_ID,
        client_secret=ENV_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )
    # refresh() 는 discovery가 자동으로 처리하지만, 즉시 토큰 확보를 위해 한 번 갱신
    creds.refresh_request = None
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def find_file(drive, folder_id: str, name: str) -> Optional[str]:
    q = f"name = '{name.replace(\"'\", \"\\'\")}' and '{folder_id}' in parents and trashed = false"
    resp = drive.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=1).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None

def upload_or_update(drive, folder_id: str, file_path: str, name: str):
    media = MediaFileUpload(file_path, mimetype="text/html", resumable=True)
    file_id = find_file(drive, folder_id, name)

    if file_id:
        # 업데이트
        return drive.files().update(
            fileId=file_id,
            media_body=media,
        ).execute()
    else:
        body = {"name": name, "parents": [folder_id], "mimeType": "text/html"}
        return drive.files().create(
            body=body, media_body=media, fields="id"
        ).execute()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="업로드할 HTML 경로")
    parser.add_argument("--folder", required=True, help="GDRIVE 폴더 ID")
    parser.add_argument("--name", default="", help="드라이브에 저장될 파일명(옵션)")
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        raise FileNotFoundError(f"파일 없음: {args.file}")

    name = args.name or os.path.basename(args.file)

    drive = make_drive_service()
    # 간헐적 5xx/쿼터 오류 대비 간단 재시도
    for attempt in range(3):
        try:
            res = upload_or_update(drive, args.folder, args.file, name)
            print(f"[GDRIVE] 업로드 완료 id={res.get('id')}")
            return
        except HttpError as e:
            print(f"[GDRIVE] 에러({e.status_code}): {e}")
            if e.status_code and int(e.status_code) >= 500 and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            raise

if __name__ == "__main__":
    main()
