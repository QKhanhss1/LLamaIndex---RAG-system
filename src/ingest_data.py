import os
import logging
import pdfplumber
import pandas as pd
from typing import List, Dict, Any, Optional
import fitz  # PyMuPDF
import pytesseract
from PIL import Image
from load_dotenv import load_dotenv

load_dotenv()

# llama_index
from llama_index.core import Document as LlamaDocument
from llama_index.core.node_parser import SentenceSplitter, SemanticSplitterNodeParser
from llama_index.embeddings.openai import OpenAIEmbedding
from pinecone import Pinecone
os.makedirs("output", exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)


# ----------- PDF Reader + Table Extractor -----------
def extract_tables_from_pdf(pdf_path: str, output_csv: str = None):
    """Extract all tables with pdfplumber"""
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
    """Extract per-page text with pdfplumber, fallback OCR via PyMuPDF + pytesseract"""
    docs = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""

            # Nếu text rỗng → OCR fallback
            if not text.strip():
                logger.info(f"⚠️ Trang {page_num} không có text → fallback OCR")
                doc = fitz.open(pdf_path)
                pix = doc[page_num - 1].get_pixmap(dpi=300)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                text = pytesseract.image_to_string(img, lang="vie+eng")  # OCR đa ngôn ngữ

            docs.append(
                {
                    "title": f"Page {page_num}",
                    "text": text,
                    "page_labels": [page_num],
                    "tables": [],
                }
            )
            logger.info(f"📄 Page {page_num} length={len(text)} chars")
    return docs


# ----------- Chunking with Semantic + Sentence Splitter -----------
def split_chunk_semantic_sentence(
    chunks: List[Dict[str, Any]],
    max_tokens: int = 1024,
    openai_api_key: Optional[str] = None,
):
    final_chunks = []

    embed_model = None
    if openai_api_key:
        embed_model = OpenAIEmbedding(
            model="text-embedding-3-small", api_key=openai_api_key
        )

    for c in chunks:
        text = c.get("text", "")
        if not text.strip():
            continue

        if embed_model:
            try:
                ss = SemanticSplitterNodeParser(
                    buffer_size=1,
                    breakpoint_percentile_threshold=95,
                    embed_model=embed_model,
                )
                doc = LlamaDocument(text=text, metadata=c)
                nodes = ss.get_nodes_from_documents([doc])
                for n in nodes:
                    final_chunks.append(
                        {
                            "title": c.get("title"),
                            "text": n.get_content(),
                            "page_labels": c.get("page_labels"),
                            "tables": c.get("tables"),
                        }
                    )
                continue
            except Exception as e:
                logger.warning(
                    f"⚠️ SemanticSplitter error: {e}, fallback to SentenceSplitter"
                )

        splitter = SentenceSplitter(
            chunk_size=max_tokens, chunk_overlap=int(max_tokens * 0.1)
        )
        parts = splitter.split_text(text)
        for p in parts:
            final_chunks.append(
                {
                    "title": c.get("title"),
                    "text": p,
                    "page_labels": c.get("page_labels"),
                    "tables": c.get("tables"),
                }
            )

    logger.info(f"After splitting: {len(final_chunks)} chunks")
    return final_chunks



# ----------- Pinecone Upsert -----------
def upsert_chunks_to_pinecone(
    chunks: List[Dict[str, Any]],
    index_name: str,
    openai_api_key: str,
    pinecone_api_key: str,
    namespace: str = "",
):
    embed_model = OpenAIEmbedding(
        model="text-embedding-3-small", api_key=openai_api_key
    )
    pc = Pinecone(api_key=pinecone_api_key)

    # Kiểm tra và tạo index nếu chưa tồn tại
    if index_name not in pc.list_indexes().names():
        logger.info(f"ℹ️ Index '{index_name}' chưa tồn tại. Đang tạo mới...")
        
        pc.create_index(
            name=index_name,
            dimension=1536,
            metric="cosine"
        )
        logger.info(f"✅ Đã tạo index '{index_name}'.")

    index = pc.Index(index_name)

    vectors = []
    for chunk in chunks:
        emb = embed_model.get_text_embedding(chunk["text"])
        vectors.append(
            {
                "id": chunk.get("id", f"{chunk['title']}_{chunk['page_labels'][0]}"),
                "values": emb,
                "metadata": {
                    "title": chunk["title"],
                    "page": chunk["page_labels"][0],
                    "text": chunk["text"],
                },
            }
        )

    logger.info(f"🔼 Upserting {len(vectors)} vectors vào Pinecone index={index_name}")
    index.upsert(vectors=vectors, namespace=namespace)
    logger.info("✅ Upsert hoàn tất.")

# ----------- Main pipeline -----------
def create_chunks_from_pdf_and_upsert(
    pdf_path: str,
    output_csv: str,
    max_tokens: int,
    openai_api_key: str,
    pinecone_api_key: str,
    pinecone_index: str,
    namespace: str = "",
):
    docs = extract_text_with_fallback(pdf_path)
    tables = extract_tables_from_pdf(pdf_path, output_csv=output_csv)
    if tables:
        docs[0]["tables"] = [tables[0].head().to_dict()]
    final_chunks = split_chunk_semantic_sentence(
        docs, max_tokens=max_tokens, openai_api_key=openai_api_key
    )

    # upsert vào Pinecone
    upsert_chunks_to_pinecone(
        final_chunks,
        index_name=pinecone_index,
        openai_api_key=openai_api_key,
        pinecone_api_key=pinecone_api_key,
        namespace=namespace,
    )


if __name__ == "__main__":
    pdf_file = "./data/Tai lieu on thi CCBHNT 2024.pdf"

    create_chunks_from_pdf_and_upsert(
        pdf_path=pdf_file,
        output_csv="./output/tables",
        max_tokens=512,
        openai_api_key= os.getenv("OPENAI_API_KEY"),
        pinecone_api_key=os.getenv("PINECONE_API_KEY"),
        pinecone_index="llamaindextest",
        namespace="default",
    )