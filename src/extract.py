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

def extract_text_with_fallback(pdf_path: str) -> List[Dict[str, Any]]:
    docs = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            if not text.strip():
                logger.info(f"⚠️ Trang {page_num} không có text → fallback OCR")
                doc = fitz.open(pdf_path)
                pix = doc[page_num - 1].get_pixmap(dpi=300)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                text = pytesseract.image_to_string(img, lang="vie+eng")
            docs.append({
                "title": f"Page {page_num}",
                "text": text,
                "page_labels": [page_num],
                "tables": [],
            })
            logger.info(f"📄 Page {page_num} length={len(text)} chars")
    return docs
