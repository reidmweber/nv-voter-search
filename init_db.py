import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

def download_db_from_gdrive():
    # Get service account credentials
    if os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON'):
        # For production - use environment variable
        creds_dict = json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON'))
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
    else:
        # For local development - use file
        credentials = service_account.Credentials.from_service_account_file(
            'path/to/your/service-account-key.json'
        )

    # Create Drive API service
    service = build('drive', 'v3', credentials=credentials)
    
    # Get file ID from environment variable
    file_id = os.getenv('GDRIVE_FILE_ID')
    if not file_id:
        raise ValueError("GDRIVE_FILE_ID environment variable is required")

    # Create destination directory
    os.makedirs('data', exist_ok=True)
    destination = os.path.join('data', 'voters.db')

    try:
        # Get file metadata
        request = service.files().get_media(fileId=file_id)
        
        # Download the file
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        
        print("Starting download...")
        while done is False:
            status, done = downloader.next_chunk()
            if status:
                print(f"Download {int(status.progress() * 100)}%")
        
        # Save the file
        fh.seek(0)
        with open(destination, 'wb') as f:
            f.write(fh.read())
            
        print(f"Database downloaded successfully to {destination}")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

if __name__ == '__main__':
    download_db_from_gdrive() 