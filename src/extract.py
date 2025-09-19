import os
import logging
import pdfplumber
import pandas as pd
import fitz  # PyMuPDF
import pytesseract
from PIL import Image
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

# ----------- Extract -----------
def extract_tables_from_pdf(pdf_path: str, output_csv: str = None):
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            page_tables = page.extract_tables()
            if not page_tables or all([not t for t in page_tables]):
                logger.info(f"❌ Trang {page_num} không có bảng nào.")
                if output_csv:
                    pd.DataFrame().to_csv(
                        f"{output_csv}_page{page_num}_table0.csv",
                        index=False,
                        encoding="utf-8-sig",
                    )
                continue
            for table_num, table in enumerate(page_tables, start=1):
                if table:
                    df = pd.DataFrame(table[1:], columns=table[0])
                    tables.append(df)
                    logger.info(
                        f"✅ Trang {page_num} - Bảng {table_num}: "
                        f"{df.shape[0]} hàng, {df.shape[1]} cột"
                    )
                    if output_csv:
                        df.to_csv(
                            f"{output_csv}_page{page_num}_table{table_num}.csv",
                            index=False,
                            encoding="utf-8-sig",
                        )
    return tables

def extract_text_with_fallback(pdf_path: str, output_txt: str = None) -> List[Dict[str, Any]]:
    """
    Extract text từ PDF với fallback OCR và tạo output file text
    Args:
        pdf_path: Đường dẫn file PDF
        output_txt: Đường dẫn output file text (optional)
    Returns:
        List[Dict]: Danh sách documents với text đã extract
    """
    docs = []
    all_text = []  # Lưu toàn bộ text để ghi file
    
    # Tạo output path nếu không được cung cấp
    if output_txt is None:
        pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_txt = os.path.join("output", f"{pdf_name}_extracted_text.txt")
    
    # Tạo thư mục output nếu chưa tồn tại
    os.makedirs(os.path.dirname(output_txt), exist_ok=True)
    
    logger.info(f"📝 Bắt đầu extract text từ: {pdf_path}")
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            if not text.strip():
                logger.info(f"⚠️ Trang {page_num} không có text → fallback OCR")
                doc = fitz.open(pdf_path)
                pix = doc[page_num - 1].get_pixmap(dpi=300)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                text = pytesseract.image_to_string(img, lang="vie+eng")
            
            # Thêm header cho mỗi trang
            page_header = f"\n{'='*50}\nTRANG {page_num}\n{'='*50}\n"
            page_text = page_header + text.strip() + "\n"
            
            docs.append({
                "title": f"Page {page_num}",
                "text": text,
                "page_labels": [page_num],
                "tables": [],
            })
            
            all_text.append(page_text)
            logger.info(f"📄 Page {page_num} length={len(text)} chars")
    
    # Ghi toàn bộ text ra file
    try:
        with open(output_txt, 'w', encoding='utf-8') as f:
            f.write(f"EXTRACTED TEXT FROM: {os.path.basename(pdf_path)}\n")
            f.write(f"Extraction Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Pages: {len(docs)}\n")
            f.write("="*80 + "\n")
            
            for page_text in all_text:
                f.write(page_text)
        
        logger.info(f"✅ Đã lưu extracted text vào: {output_txt}")
        logger.info(f"📊 Tổng cộng: {len(docs)} trang, {sum(len(doc['text']) for doc in docs)} ký tự")
        
    except Exception as e:
        logger.error(f"❌ Lỗi ghi file text: {e}")
    
    return docs
