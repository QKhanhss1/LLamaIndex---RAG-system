from llama_index.readers.google import GoogleDriveReader
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

FOLDER_ID = "1vJvuf0DngYWGMRHrLtxploViAjtM9jzS"  # Thay bằng folder ID của bạn
CREDENTIALS_PATH = "credentials.json"  # Đường dẫn tới file credentials

def log_document_info(docs):
    """Log thông tin chi tiết từng document"""
    logger.info("========== 📂 DANH SÁCH FILE TRÍCH XUẤT ==========")
    for i, doc in enumerate(docs, start=1):
        doc_id = getattr(doc, "doc_id", "N/A")
        text_preview = getattr(doc, "text", str(doc))[:300].replace("\n", " ")
        logger.info(f"[{i}] File ID: {doc_id}")
        logger.info(f"    Preview: {text_preview}")
        logger.info("-" * 50)

def main():
    logger.info("🚀 Bắt đầu extract dữ liệu từ Google Drive...")
    reader = GoogleDriveReader(credentials_path=CREDENTIALS_PATH)
    docs = reader.load_data(folder_id=FOLDER_ID)

    logger.info(f"✅ Tổng số file đã extract: {len(docs)}")
    log_document_info(docs)

if __name__ == "__main__":
    main()
