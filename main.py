# ingest.py
import os
from llama_index.core import SimpleDirectoryReader

import logging
from typing import List
import pinecone

from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.vector_stores.pinecone import PineconeVectorStore
from llama_index.core import StorageContext, VectorStoreIndex
from typing import List
from llama_index.core.schema import BaseNode

from llama_index.core import Document
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.node_parser import SemanticSplitterNodeParser
from llama_index.embeddings.openai import OpenAIEmbedding

from dotenv import load_dotenv

load_dotenv()

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
INDEX_NAME = os.getenv("PINECONE_INDEX_NAME")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)


def ingest_pdfs(data_dir: str = "./data"):
    """
    Đọc tất cả file PDF trong thư mục data/ 
    và trả về list Document objects
    """
    if not os.path.exists(data_dir):
        raise ValueError(f"❌ Thư mục {data_dir} không tồn tại!")

    # SimpleDirectoryReader mặc định sử dụng pypdf để đọc file PDF
    reader = SimpleDirectoryReader(
        input_dir=data_dir,
        required_exts=[".pdf"],   # chỉ đọc file PDF
        recursive=True            # đọc cả sub-folder nếu có
    )

    docs = reader.load_data()
    print(f"✅ Đã ingest {len(docs)} file PDF từ thư mục {data_dir}")
    return docs

def chunk_documents(docs: List[Document], use_semantic: bool = True):
    """
    Chunking dữ liệu với SentenceSplitter + (tùy chọn) SemanticSplitterNodeParser
    """

    if not docs:
        logger.warning("❌ Không có document nào để chunking.")
        return []

    # Step 1: Sentence-based chunking
    logger.info("👉 Bắt đầu chunking bằng SentenceSplitter...")
    sentence_splitter = SentenceSplitter(chunk_size=512, chunk_overlap=50)
    sentence_nodes = sentence_splitter.get_nodes_from_documents(docs)
    logger.info(f"✅ SentenceSplitter tạo ra {len(sentence_nodes)} chunks.")

    if not use_semantic:
        return sentence_nodes

    # Step 2: Semantic-based chunking
    logger.info("👉 Bắt đầu chunking bằng SemanticSplitterNodeParser...")
    embed_model = OpenAIEmbedding(model="text-embedding-3-small")
    semantic_splitter = SemanticSplitterNodeParser(
        embed_model=embed_model,
        buffer_size=1,
        breakpoint_percentile_threshold=95,
    )

    semantic_nodes = semantic_splitter.get_nodes_from_documents(docs)
    logger.info(f"✅ SemanticSplitter tạo ra {len(semantic_nodes)} chunks.")

    return semantic_nodes
def push_to_pinecone(nodes: List[BaseNode]):
    """
    Tạo embeddings cho các nodes và lưu vào Pinecone index 'ragflow'
    """
    if not nodes:
        logger.warning("❌ Không có nodes nào để embed.")
        return None
    pc = pinecone.Pinecone(api_key=PINECONE_API_KEY)
    if INDEX_NAME not in pc.list_indexes().names():
    # Nếu index chưa tồn tại → tạo mới
        logger.info(f"👉 Chưa có index '{INDEX_NAME}', tạo mới...")
        pc.create_index(
            INDEX_NAME,
            dimension=1536,  # text-embedding-3-small có dimension 1536
            metric="cosine"
        )

    pinecone_index = pc.Index(INDEX_NAME)

    # Embedding model
    embed_model = OpenAIEmbedding(model="text-embedding-3-small")

    # Tạo VectorStore kết nối với Pinecone
    vector_store = PineconeVectorStore(pinecone_index)
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    # Build index và push lên Pinecone
    logger.info("👉 Đang tạo VectorStoreIndex và đẩy embeddings vào Pinecone...")
    index = VectorStoreIndex(
        nodes,
        storage_context=storage_context,
        embed_model=embed_model
    )

    logger.info(f"✅ Đã push {len(nodes)} chunks vào Pinecone index '{INDEX_NAME}'.")
    return index

if __name__ == "__main__":
    docs = ingest_pdfs(data_dir="./data")
    nodes = chunk_documents(docs, use_semantic=True)
    push_to_pinecone(nodes)
    logger.info(f"Tổng số nodes sau chunking: {len(nodes)}")
    if nodes:
        logger.info("📄 Mẫu chunk:")
        logger.info(nodes[0].get_content()[:500])   