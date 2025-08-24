# scripts/fetch_from_gdrive.py
# -*- coding: utf-8 -*-
import os, json, requests, sys

TOKEN_URL = "https://oauth2.googleapis.com/token"
LIST_URL  = "https://www.googleapis.com/drive/v3/files"
GET_URL   = "https://www.googleapis.com/drive/v3/files/{fid}?alt=media"
EXPORT_URL= "https://www.googleapis.com/drive/v3/files/{fid}/export?mimeType=text/csv"

CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN")
ROOT_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")
OUT_DIR = os.getenv("DATA_DIR","./data/daily")

def must(cond, msg):
    if not cond:
        print(f"[GDRIVE] {msg}")
        sys.exit(1)

def get_access_token():
    r = requests.post(TOKEN_URL, data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "grant_type": "refresh_token",
    }, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def list_children(token, folder_id):
    files = []
    page_token = None
    while True:
        params = {
            "q": f"'{folder_id}' in parents and trashed=false",
            "fields": "nextPageToken, files(id,name,mimeType)",
            "pageSize": 1000,
        }
        if page_token:
            params["pageToken"] = page_token
        r = requests.get(LIST_URL, headers={"Authorization": f"Bearer {token}"}, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        files.extend(data.get("files", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return files

def walk(token, folder_id):
    all_files = []
    stack = [folder_id]
    while stack:
        fid = stack.pop()
        children = list_children(token, fid)
        for f in children:
            if f["mimeType"] == "application/vnd.google-apps.folder":
                stack.append(f["id"])
            else:
                all_files.append(f)
    return all_files

def download_file(token, fid, name, mime):
    os.makedirs(OUT_DIR, exist_ok=True)
    # Google Sheets → export CSV
    if mime == "application/vnd.google-apps.spreadsheet":
        url = EXPORT_URL.format(fid=fid)
        ext = ".csv" if not name.lower().endswith(".csv") else ""
        path = os.path.join(OUT_DIR, name + ext)
    else:
        url = GET_URL.format(fid=fid)
        path = os.path.join(OUT_DIR, name)
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, stream=True, timeout=120)
    r.raise_for_status()
    with open(path, "wb") as f:
        for ch in r.iter_content(chunk_size=65536):
            if ch: f.write(ch)
    return path

def main():
    must(CLIENT_ID and CLIENT_SECRET and REFRESH_TOKEN and ROOT_FOLDER_ID, 
         "GOOGLE_* / GDRIVE_FOLDER_ID 시크릿을 확인하세요.")
    token = get_access_token()
    files = walk(token, ROOT_FOLDER_ID)
    print(f"[GDRIVE] found files (recursive): {len(files)}")

    saved = 0
    for f in files:
        mime = f["mimeType"]
        name = f["name"]
        # CSV 또는 구글 시트만 저장
        if mime in ("text/csv", "application/vnd.ms-excel", "application/octet-stream") or \
           mime == "application/vnd.google-apps.spreadsheet" or \
           name.lower().endswith(".csv"):
            path = download_file(token, f["id"], name, mime)
            print(f"  - saved: {path}")
            saved += 1
    print(f"[GDRIVE] saved CSV-like files: {saved}")

if __name__ == "__main__":
    main()
