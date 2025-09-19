from flask import Flask, request, jsonify
import os
import sys
import logging
from datetime import datetime

# Thêm src folder vào path để import Load_ggdrive
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from Load_ggdrive import download_pdf_from_drive

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

# Khởi tạo Flask app
app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    """Trang chủ API"""
    return jsonify({
        "message": "🚀 Google Drive PDF Downloader API",
        "version": "1.0.0",
        "endpoints": {
            "GET /": "Trang chủ API",
            "POST /download": "Tải file từ Google Drive",
            "GET /health": "Kiểm tra trạng thái API",
            "GET /files": "Liệt kê files trong thư mục data"
        },
        "author": "RAG System",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/health', methods=['GET'])
def health_check():
    """Kiểm tra trạng thái API"""
    try:
        # Kiểm tra credentials file
        credentials_path = "credentials.json"
        credentials_exists = os.path.exists(credentials_path)
        
        # Kiểm tra thư mục data
        data_dir = "data"
        data_exists = os.path.exists(data_dir)
        
        return jsonify({
            "status": "healthy" if credentials_exists and data_exists else "warning",
            "timestamp": datetime.now().isoformat(),
            "checks": {
                "credentials_file": credentials_exists,
                "data_directory": data_exists,
                "python_version": sys.version
            }
        })
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/download', methods=['POST'])
def download_file():
    """
    Tải file từ Google Drive
    
    Body JSON:
    {
        "file_id": "string (required)",
        "file_name": "string (optional)"
    }
    """
    try:
        # Lấy dữ liệu từ request
        data = request.get_json()
        
        if not data:
            return jsonify({
                "error": "Missing JSON body",
                "message": "Vui lòng gửi dữ liệu JSON"
            }), 400
        
        file_id = data.get('file_id')
        file_name = data.get('file_name', f'downloaded_file_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
        
        # Validate file_id
        if not file_id:
            return jsonify({
                "error": "Missing file_id",
                "message": "file_id là bắt buộc",
                "example": {
                    "file_id": "1H9I25au5fVMY7zrai5ZvwQx9eBDXc7k8",
                    "file_name": "my-document"
                }
            }), 400
        
        logger.info(f"🔄 API Request: Tải file_id={file_id}, file_name={file_name}")
        
        # Gọi function download
        downloaded_path = download_pdf_from_drive(file_id, file_name)
        
        if downloaded_path:
            # Lấy thông tin file
            file_size = os.path.getsize(downloaded_path) if os.path.exists(downloaded_path) else 0
            
            return jsonify({
                "success": True,
                "message": "Tải file thành công!",
                "data": {
                    "file_path": downloaded_path,
                    "file_name": os.path.basename(downloaded_path),
                    "file_size_mb": round(file_size / (1024 * 1024), 2),
                    "download_time": datetime.now().isoformat()
                }
            }), 200
        else:
            return jsonify({
                "success": False,
                "error": "Download failed",
                "message": "Không thể tải file. Kiểm tra logs để biết chi tiết."
            }), 500
            
    except Exception as e:
        logger.error(f"❌ API Error: {e}")
        return jsonify({
            "success": False,
            "error": "Internal server error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/files', methods=['GET'])
def list_files():
    """Liệt kê tất cả files trong thư mục data"""
    try:
        data_dir = "data"
        
        if not os.path.exists(data_dir):
            return jsonify({
                "files": [],
                "count": 0,
                "message": "Thư mục data không tồn tại"
            })
        
        # Lấy tất cả files
        all_files = []
        for file_name in os.listdir(data_dir):
            file_path = os.path.join(data_dir, file_name)
            if os.path.isfile(file_path):
                file_stat = os.stat(file_path)
                all_files.append({
                    "name": file_name,
                    "size_mb": round(file_stat.st_size / (1024 * 1024), 2),
                    "modified_time": datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                    "is_pdf": file_name.lower().endswith('.pdf')
                })
        
        # Sắp xếp theo thời gian modified
        all_files.sort(key=lambda x: x['modified_time'], reverse=True)
        
        pdf_count = sum(1 for f in all_files if f['is_pdf'])
        
        return jsonify({
            "files": all_files,
            "count": len(all_files),
            "pdf_count": pdf_count,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"❌ List files error: {e}")
        return jsonify({
            "error": "Cannot list files",
            "message": str(e)
        }), 500

@app.errorhandler(404)
def not_found(error):
    """Handler cho 404 errors"""
    return jsonify({
        "error": "Not Found",
        "message": "API endpoint không tồn tại",
        "available_endpoints": ["/", "/download", "/health", "/files"]
    }), 404

@app.errorhandler(500)
def internal_error(error):
    """Handler cho 500 errors"""
    return jsonify({
        "error": "Internal Server Error",
        "message": "Lỗi server nội bộ"
    }), 500

if __name__ == '__main__':
    logger.info("🚀 Khởi động Google Drive PDF Downloader API...")
    logger.info("📚 Endpoints available:")
    logger.info("   GET  / - Trang chủ")
    logger.info("   POST /download - Tải file từ Google Drive")
    logger.info("   GET  /health - Kiểm tra trạng thái")
    logger.info("   GET  /files - Liệt kê files trong data/")
    
    # Chạy Flask app
    app.run(
        host='0.0.0.0',  # Cho phép truy cập từ bên ngoài
        port=5000,       # Port 5000
        debug=True       # Enable debug mode
    )