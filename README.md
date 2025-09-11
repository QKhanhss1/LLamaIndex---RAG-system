# RAG LlamaIndex Pinecone

## Mô tả

Dự án này sử dụng LlamaIndex, Pinecone và các công cụ AI để xây dựng hệ thống truy vấn tài liệu PDF, trích xuất bảng, chunking ngữ nghĩa, và lưu trữ vector cho tìm kiếm thông minh.

## Cấu trúc thư mục
- `src/` : Chứa các script chính (ingest, retrieve, ...)
- `data/` : Chứa các file PDF đầu vào
- `output/` : Chứa file kết quả trích xuất (text, bảng, ...)
- `requirements.txt` : Danh sách các package cần thiết
- `.gitignore` : Loại trừ file không cần thiết khi dùng Git

## Hướng dẫn sử dụng
1. **Cài đặt môi trường**
   ```
   pip install -r requirements.txt
   ```
2. **Ingest dữ liệu PDF**
   ```
   python src/ingest_data.py
   ```
   - Kết quả: file text từng trang, bảng CSV, và vector lưu lên Pinecone
3. **Truy vấn dữ liệu**
   ```
   python src/retrieve.py
   ```
   - Truy vấn ngữ nghĩa + keyword + rerank

## Yêu cầu hệ thống
- Python >= 3.10
- Đã cài đặt Poppler (cho pdf2image)
- Đã cài đặt Tesseract OCR (cho pytesseract, hỗ trợ tiếng Việt)

## Liên hệ
- Tác giả: [Tên của bạn]
- Email: [Email của bạn]
