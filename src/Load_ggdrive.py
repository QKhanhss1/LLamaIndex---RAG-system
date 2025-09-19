

import os
import datetime
import logging
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = 'data'
CREDENTIALS_FILE = 'credentials.json'  # Đường dẫn tới file service account

def download_pdf_from_drive(file_id: str, file_name: str):
    """
    Tải file PDF từ Google Drive bằng service account
    Args:
        file_id: ID của file trên Google Drive
        file_name: Tên file muốn lưu (không cần đuôi mở rộng)
    Returns:
        str: Đường dẫn file đã lưu hoặc None nếu lỗi
    """
    logger.info(f"🔄 Bắt đầu tải file_id: {file_id}")
    
    # Kiểm tra input
    if not file_id or not file_id.strip():
        logger.error("❌ File ID không được để trống!")
        return None
    
    if not file_name or not file_name.strip():
        logger.error("❌ Tên file không được để trống!")
        return None
    
    # Kiểm tra file credentials
    if not os.path.exists(CREDENTIALS_FILE):
        logger.error(f"❌ Không tìm thấy file credentials: {CREDENTIALS_FILE}")
        logger.error("💡 Hãy đảm bảo file service account credentials.json tồn tại!")
        return None
    
    try:
        # Xác thực với Google Drive API
        logger.info("🔐 Đang xác thực với Google Drive API...")
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        service = build('drive', 'v3', credentials=creds)
        logger.info("✅ Xác thực thành công!")
        
    except FileNotFoundError:
        logger.error(f"❌ File credentials không tồn tại: {CREDENTIALS_FILE}")
        return None
    except ValueError as e:
        logger.error(f"❌ File credentials không hợp lệ: {e}")
        logger.error("💡 Kiểm tra định dạng JSON của file credentials!")
        return None
    except Exception as e:
        logger.error(f"❌ Lỗi xác thực Google Drive API: {e}")
        return None

    # Tạo timestamp và đường dẫn
    try:
        now = datetime.datetime.now()
        timestamp = now.strftime('%Y-%m-%d_%H-%M-%S')
        destination = os.path.join(DATA_DIR, f"{file_name}_{timestamp}.pdf")
        os.makedirs(DATA_DIR, exist_ok=True)
        logger.info(f"� Đường dẫn lưu file: {destination}")
    except Exception as e:
        logger.error(f"❌ Lỗi tạo đường dẫn file: {e}")
        return None

    # Lấy thông tin file để xác định loại
    try:
        logger.info("🔍 Đang lấy thông tin file...")
        file_info = service.files().get(fileId=file_id, fields='mimeType, name, size').execute()
        mime_type = file_info.get('mimeType')
        original_name = file_info.get('name', 'Unknown')
        file_size = file_info.get('size', 'Unknown')
        
        logger.info(f"📄 Tên file gốc: {original_name}")
        logger.info(f"📏 Kích thước: {file_size} bytes" if file_size != 'Unknown' else "📏 Kích thước: không xác định")
        logger.info(f"🏷️ MIME Type: {mime_type}")
        
    except HttpError as e:
        if e.resp.status == 404:
            logger.error(f"❌ File không tồn tại hoặc không có quyền truy cập: {file_id}")
            logger.error("💡 Kiểm tra lại file_id hoặc quyền truy cập file!")
        elif e.resp.status == 403:
            logger.error(f"❌ Không có quyền truy cập file: {file_id}")
            logger.error("💡 File có thể bị hạn chế quyền truy cập hoặc service account chưa được chia sẻ!")
        else:
            logger.error(f"❌ HTTP Error {e.resp.status}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Lỗi lấy thông tin file: {e}")
        return None
    
    # Kiểm tra loại file và chọn phương thức tải phù hợp
    try:
        if mime_type in ['application/vnd.google-apps.document', 
                        'application/vnd.google-apps.spreadsheet', 
                        'application/vnd.google-apps.presentation']:
            logger.info(f"� Phát hiện Google {mime_type.split('.')[-1]}, đang export sang PDF...")
            request = service.files().export_media(fileId=file_id, mimeType='application/pdf')
        else:
            logger.info(f"� Đang tải file thường (MIME: {mime_type})...")
            request = service.files().get_media(fileId=file_id)
        
        # Tải file với progress tracking
        logger.info("🔄 Bắt đầu download...")
        with open(destination, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                try:
                    status, done = downloader.next_chunk()
                    progress = int(status.progress() * 100)
                    if progress % 20 == 0 or done:  # Log mỗi 20% hoặc khi hoàn thành
                        logger.info(f"📊 Tiến độ: {progress}%")
                except Exception as chunk_error:
                    logger.error(f"❌ Lỗi khi tải chunk: {chunk_error}")
                    raise
    
    except HttpError as e:
        if e.resp.status == 403:
            if "Export only supports Docs Editors files" in str(e):
                logger.error("❌ Không thể export file này sang PDF!")
                logger.error("💡 File không phải là Google Docs/Sheets/Slides!")
            elif "Rate Limit Exceeded" in str(e):
                logger.error("❌ Vượt quá giới hạn tải về! Thử lại sau ít phút.")
            else:
                logger.error(f"❌ Không có quyền truy cập: {e}")
        elif e.resp.status == 404:
            logger.error("❌ File không tồn tại hoặc đã bị xóa!")
        elif e.resp.status == 429:
            logger.error("❌ Quá nhiều requests! Vui lòng thử lại sau.")
        else:
            logger.error(f"❌ HTTP Error {e.resp.status}: {e}")
        return None
        
    except PermissionError:
        logger.error(f"❌ Không có quyền ghi file vào: {destination}")
        logger.error("💡 Kiểm tra quyền ghi thư mục data/ hoặc đóng file nếu đang mở!")
        return None
        
    except IOError as e:
        logger.error(f"❌ Lỗi I/O khi lưu file: {e}")
        logger.error("💡 Kiểm tra dung lượng ổ đĩa còn trống!")
        return None
        
    except Exception as e:
        logger.error(f"❌ Lỗi không xác định khi tải file: {e}")
        logger.exception("Chi tiết lỗi:")
        return None

    # Kiểm tra file đã tải
    try:
        if os.path.exists(destination) and os.path.getsize(destination) > 0:
            file_size_mb = os.path.getsize(destination) / (1024 * 1024)
            logger.info(f"✅ Đã tải thành công file vào: {destination}")
            logger.info(f"📏 Kích thước file: {file_size_mb:.2f} MB")
            logger.info(f"📅 Thời gian tải: {now.strftime('%d/%m/%Y lúc %H:%M:%S')}")
            return destination
        else:
            logger.error("❌ File tải về bị rỗng hoặc không tồn tại!")
            if os.path.exists(destination):
                os.remove(destination)
                logger.info("🗑️ Đã xóa file rỗng")
            return None
    except Exception as e:
        logger.error(f"❌ Lỗi kiểm tra file đã tải: {e}")
        return None

if __name__ == "__main__":
    file_id = input("Nhập file_id Google Drive: ")
    file_name = input("Nhập tên file (không cần đuôi mở rộng): ")
    download_pdf_from_drive(file_id, file_name)
