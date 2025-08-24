# scripts/fetch_from_gdrive.py
# -*- coding: utf-8 -*-
import os, json, requests

TOKEN_URL = "https://oauth2.googleapis.com/token"
LIST_URL  = "https://www.googleapis.com/drive/v3/files"
GET_URL   = "https://www.googleapis.com/drive/v3/files/{fid}?alt=media"

CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN")
FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")
OUT_DIR = os.getenv("DATA_DIR","./data/daily")

def get_access_token():
    r = requests.post(TOKEN_URL, data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "grant_type": "refresh_token",
    }, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def list_csv_files(token):
    q = f"'{FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false"
    params = {
        "q": q,
        "fields": "files(id,name,modifiedTime,mimeType)",
        "pageSize": 1000,
        "orderBy": "modifiedTime desc",
    }
    r = requests.get(LIST_URL, headers={"Authorization": f"Bearer {token}"}, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("files", [])

def download(token, fid, name):
    os.makedirs(OUT_DIR, exist_ok=True)
    r = requests.get(GET_URL.format(fid=fid), headers={"Authorization": f"Bearer {token}"}, stream=True, timeout=60)
    r.raise_for_status()
    path = os.path.join(OUT_DIR, name)
    with open(path, "wb") as f:
        for ch in r.iter_content(chunk_size=65536):
            if ch: f.write(ch)
    return path

def main():
    if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, FOLDER_ID]):
        raise SystemExit("!! GOOGLE_* / GDRIVE_FOLDER_ID 시크릿을 확인하세요.")
    token = get_access_token()
    files = list_csv_files(token)
    print(f"[GDRIVE] csv files: {len(files)}")
    for f in files:
        path = download(token, f["id"], f["name"])
        print(f"  - saved: {path}")

if __name__ == "__main__":
    main()
