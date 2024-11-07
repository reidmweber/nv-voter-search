import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
from .db import DB_PATH, init_db

def download_from_gdrive():
    """Download database from Google Drive"""
    if os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON'):
        creds_dict = json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON'))
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
    else:
        credentials = service_account.Credentials.from_service_account_file(
            'keys/voters-440923-c7b21ab1012e.json'
        )

    service = build('drive', 'v3', credentials=credentials)
    file_id = os.getenv('GDRIVE_FILE_ID')
    
    if not file_id:
        raise ValueError("GDRIVE_FILE_ID environment variable is required")

    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        
        print(f"Starting download to {DB_PATH}...")
        while done is False:
            status, done = downloader.next_chunk()
            if status:
                print(f"Download {int(status.progress() * 100)}%")
        
        fh.seek(0)
        with open(DB_PATH, 'wb') as f:
            f.write(fh.read())
            
        print(f"Database downloaded successfully to {DB_PATH}")
        
    except Exception as e:
        print(f"Download error: {e}")
        raise

def upload_to_gdrive():
    """Upload database to Google Drive"""
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found at {DB_PATH}")

    if os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON'):
        creds_dict = json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON'))
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
    else:
        credentials = service_account.Credentials.from_service_account_file(
            'keys/voters-440923-c7b21ab1012e.json'
        )

    service = build('drive', 'v3', credentials=credentials)
    file_id = os.getenv('GDRIVE_FILE_ID')
    
    if not file_id:
        raise ValueError("GDRIVE_FILE_ID environment variable is required")

    try:
        media = MediaFileUpload(DB_PATH, mimetype='application/x-sqlite3')
        file = service.files().update(
            fileId=file_id,
            media_body=media
        ).execute()
        
        print(f"Database uploaded successfully to Google Drive with ID: {file.get('id')}")
        
    except Exception as e:
        print(f"Upload error: {e}")
        raise 