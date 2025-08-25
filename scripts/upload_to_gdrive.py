# scripts/upload_to_gdrive.py
# -*- coding: utf-8 -*-
import os, sys, mimetypes
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def get_service():
    creds = Credentials(
        None,
        refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def upsert(service, folder_id, path):
    name = os.path.basename(path)
    mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
    q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    existing = service.files().list(q=q, fields="files(id)").execute().get("files", [])
    media = MediaFileUpload(path, mimetype=mime, resumable=False)
    if existing:
        file_id = existing[0]["id"]
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"Updated: {name} ({file_id})")
    else:
        body = {"name": name, "parents": [folder_id]}
        file = service.files().create(body=body, media_body=media, fields="id").execute()
        print(f"Uploaded: {name} ({file['id']})")

if __name__ == "__main__":
    folder_id = os.environ["GDRIVE_FOLDER_ID"]
    svc = get_service()
    for p in sys.argv[1:]:
        upsert(svc, folder_id, p)
