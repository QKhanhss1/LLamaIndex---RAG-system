

import os
import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

DATA_DIR = 'data'
CREDENTIALS_FILE = 'credentials.json'  # Đường dẫn tới file service account

def download_pdf_from_drive(file_id: str, file_name: str):
    """
    Tải file PDF từ Google Drive bằng service account
    Args:
        file_id: ID của file trên Google Drive
        file_name: Tên file muốn lưu (không cần đuôi mở rộng)
    Returns:
        str: Đường dẫn file đã lưu
    """
    # Xác thực với Google Drive API
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    service = build('drive', 'v3', credentials=creds)

    # Tạo timestamp
    now = datetime.datetime.now()
    timestamp = now.strftime('%Y-%m-%d_%H-%M-%S')
    destination = os.path.join(DATA_DIR, f"{file_name}_{timestamp}.pdf")
    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"🔄 Đang tải file_id: {file_id}")
    print(f"📁 Lưu vào: {destination}")

    # Lấy thông tin file để xác định loại
    file_info = service.files().get(fileId=file_id, fields='mimeType, name').execute()
    mime_type = file_info.get('mimeType')
    
    # Kiểm tra loại file và chọn phương thức tải phù hợp
    try:
        if mime_type in ['application/vnd.google-apps.document', 
                        'application/vnd.google-apps.spreadsheet', 
                        'application/vnd.google-apps.presentation']:
            print(f"🔄 Phát hiện Google {mime_type.split('.')[-1]}, export sang PDF...")
            request = service.files().export_media(fileId=file_id, mimeType='application/pdf')
        else:
            print(f"🔄 Tải file thường ({mime_type})...")
            request = service.files().get_media(fileId=file_id)
        
        with open(destination, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"Download {int(status.progress() * 100)}%.")
    
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        return None

    print(f"✅ Đã tải thành công file vào: {destination}")
    print(f"📅 Thời gian tải: {now.strftime('%d/%m/%Y lúc %H:%M:%S')}")
    return destination

if __name__ == "__main__":
    file_id = input("Nhập file_id Google Drive: ")
    file_name = input("Nhập tên file (không cần đuôi mở rộng): ")
    download_pdf_from_drive(file_id, file_name)
