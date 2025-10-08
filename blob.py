from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from logger import Logger
from typing import Any
import os
import io


log = Logger()


class Blob:
    def __init__(self) -> None:
        self.SCOPES = ['https://www.googleapis.com/auth/drive.file']
        self.service = self.authenticate()
        self.folder_id = {
            "Documents": '1iCuSdEY7ygZm9-ua9ljlvhGNLSPmynxm',
            "DocumentImages": '1plT0jLYw83MVvler7LO8QUkRkiHBJVWO',
            "ChatImages": '1EKAmV6S-LfLSL5r43JoLWukivArPdxNE',
            "Logs": '1D0ZoO0A2SxEIMj2s7W-sNhGh1SEdg_Vm'
        }
    
    def authenticate(self) -> Any | None:
        creds = None
        try:
            if os.path.exists('./etc/secrets/token.json'):
                creds = Credentials.from_authorized_user_file('./etc/secrets/token.json', self.SCOPES)

            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        './etc/secrets/credentials.json', self.SCOPES)
                    creds = flow.run_local_server(port=0)

                with open('./etc/secrets/token.json', 'w') as token:
                    token.write(creds.to_json())
            
            log.log_event("SYSTEM", f"[BLOB] Authentication successful.")
            return build('drive', 'v3', credentials=creds, cache_discovery=False)
        except Exception as excp:
            log.log_event("SYSTEM", f"[BLOB] Authentication failed. {excp}")
            return None
        
    def upload_file(self, service, file_path, folder_name) -> dict | None:
        try:
            folder_id = self.folder_id.get(folder_name)
            file_name = os.path.basename(file_path)
            
            if file_path.lower().endswith('.pdf'):
                mime_type = 'application/pdf'
            elif file_path.lower().endswith(('.png', '.jpg', '.jpeg')):
                mime_type = 'image/jpeg' if file_path.lower().endswith(('.jpg', '.jpeg')) else 'image/png'
            else:
                mime_type = 'application/octet-stream'
            
            file_metadata = {'name': file_name}
            
            # Check if file already exists in the folder
            existing_file_id = None
            if folder_id:
                query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
                results = service.files().list(q=query, fields="files(id, name)").execute()
                files = results.get('files', [])
                if files:
                    existing_file_id = files[0]['id']
            
            media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
            
            if existing_file_id:
                # Update existing file
                file = service.files().update(
                    fileId=existing_file_id,
                    media_body=media,
                    fields='id, name, webViewLink'
                ).execute()
                log.log_event("SYSTEM", f"[BLOB] File updated successfully. {file.get('name')}")
            else:
                # Create new file
                if folder_id:
                    file_metadata['parents'] = [folder_id]
                
                file = service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id, name, webViewLink'
                ).execute()
                log.log_event("SYSTEM", f"[BLOB] File uploaded successfully. {file.get('name')}")
            
            return {
                "file_name": file.get("name"),
                "file_id": file.get("id"),
                "file_link": file.get("webViewLink")
            }
        except Exception as excp:
            log.log_event("SYSTEM", f"[BLOB] Uploading failed. {excp}")
            return None
        # print(f'Uploaded: {file.get("name")} (ID: {file.get("id")})')
        # print(f'Link: {file.get("webViewLink")}')
        # return file.get('id')

    def download_file(self, service, file_id, destination_path) -> bool | None:
        try:
            request = service.files().get_media(fileId=file_id)
            
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f'Download {int(status.progress() * 100)}%')
            
            # Write to file
            with open(destination_path, 'wb') as f:
                f.write(fh.getvalue())
            
            log.log_event("SYSTEM", f"[BLOB] Downloaded to: {destination_path}")
            # print(f'Downloaded to: {destination_path}')
            return True
        except Exception as excp:
            log.log_event("SYSTEM", f"[BLOB] Downloading failed. {excp}")
            return None
    
    def list_files(self, service, query=None, page_size=10) -> str | None:
        try:
            results = service.files().list(
                pageSize=page_size,
                fields="files(id, name, mimeType, createdTime)",
                q=query
            ).execute()
            
            files = results.get('files', [])
            
            if not files:
                log.log_event("SYSTEM", f"[BLOB] No files found.")
                return None
            else:
                print('Files:')
                for file in files:
                    print(f"{file['name']} ({file['id']}) - {file['mimeType']}")
            
            return files
        except Exception as excp:
            log.log_event("SYSTEM", f"[BLOB] Fetching files failed. {excp}")
            return None
    

        
# Define the scopes
# SCOPES = ['https://www.googleapis.com/auth/drive.file']

# def authenticate():
#     """Authenticate and return Google Drive service"""
#     creds = None
    
#     # Token file stores user's access and refresh tokens
#     if os.path.exists('token.json'):
#         creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
#     # If no valid credentials, let user log in
#     if not creds or not creds.valid:
#         if creds and creds.expired and creds.refresh_token:
#             creds.refresh(Request())
#         else:
#             flow = InstalledAppFlow.from_client_secrets_file(
#                 'credentials.json', SCOPES)
#             creds = flow.run_local_server(port=0)
        
#         # Save credentials for next run
#         with open('token.json', 'w') as token:
#             token.write(creds.to_json())
    
#     return build('drive', 'v3', credentials=creds, cache_discovery=False)

# def upload_file(service, file_path, folder_id=None):
#     """Upload a file to Google Drive"""
#     file_name = os.path.basename(file_path)
    
#     # Determine MIME type based on extension
#     if file_path.lower().endswith('.pdf'):
#         mime_type = 'application/pdf'
#     elif file_path.lower().endswith(('.png', '.jpg', '.jpeg')):
#         mime_type = 'image/jpeg' if file_path.lower().endswith(('.jpg', '.jpeg')) else 'image/png'
#     else:
#         mime_type = 'application/octet-stream'
    
#     file_metadata = {'name': file_name}
    
#     # If folder_id provided, upload to that folder
#     if folder_id:
#         file_metadata['parents'] = [folder_id]
    
#     media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
    
#     file = service.files().create(
#         body=file_metadata,
#         media_body=media,
#         fields='id, name, webViewLink'
#     ).execute()
    
#     print(f'Uploaded: {file.get("name")} (ID: {file.get("id")})')
#     print(f'Link: {file.get("webViewLink")}')
#     return file.get('id')

# def download_file(service, file_id, destination_path):
#     """Download a file from Google Drive"""
#     request = service.files().get_media(fileId=file_id)
    
#     fh = io.BytesIO()
#     downloader = MediaIoBaseDownload(fh, request)
    
#     done = False
#     while not done:
#         status, done = downloader.next_chunk()
#         print(f'Download {int(status.progress() * 100)}%')
    
#     # Write to file
#     with open(destination_path, 'wb') as f:
#         f.write(fh.getvalue())
    
#     print(f'Downloaded to: {destination_path}')

# def list_files(service, query=None, page_size=10):
#     """List files in Google Drive"""
#     results = service.files().list(
#         pageSize=page_size,
#         fields="files(id, name, mimeType, createdTime)",
#         q=query
#     ).execute()
    
#     files = results.get('files', [])
    
#     if not files:
#         print('No files found.')
#     else:
#         print('Files:')
#         for file in files:
#             print(f"{file['name']} ({file['id']}) - {file['mimeType']}")
    
#     return files

# def search_pdfs(service):
#     """Search for PDF files"""
#     query = "mimeType='application/pdf'"
#     return list_files(service, query=query)

# def search_images(service):
#     """Search for image files"""
#     query = "mimeType contains 'image/'"
#     return list_files(service, query=query)

# def create_folder(service, folder_name, parent_folder_id=None):
#     """Create a folder in Google Drive"""
#     file_metadata = {
#         'name': folder_name,
#         'mimeType': 'application/vnd.google-apps.folder'
#     }
    
#     if parent_folder_id:
#         file_metadata['parents'] = [parent_folder_id]
    
#     folder = service.files().create(
#         body=file_metadata,
#         fields='id, name'
#     ).execute()
    
#     print(f'Created folder: {folder.get("name")} (ID: {folder.get("id")})')
#     return folder.get('id')

# # Example usage
# if __name__ == '__main__':
# #     # Authenticate
#     service = authenticate()
    
#     # Upload a PDF
#     # upload_file(service, 'example.pdf')
    
#     # Upload an image
#     # upload_file(service, 'example.png')
   
#     # Create a folder and upload print(
    # print(create_folder(service, 'Documents'))
    # print(create_folder(service, 'DocumentImages'))
    # print(create_folder(service, 'ChatImages'))
    # print(create_folder(service, 'Logs'))
#     # upload_file(service, 'REC Policy Manual 2024.pdf', folder_id=folder_id)
    
#     # List all files
#     # list_files(service)
    
#     # Search for PDFs only
#     # search_pdfs(service)
    
#     # Search for images only
#     # search_images(service)
    
#     # Download a file (replace FILE_ID with actual ID)
#     download_file(service, '1Rg2G1ygaNUmi-j2GfH77mIxJ1bNmGMzD', 'downloaded_file.pdf')