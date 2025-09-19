from typing import List, Dict, Any
from llama_index.embeddings.openai import OpenAIEmbedding
from pinecone import Pinecone
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

# ----------- Load -----------
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
        vectors.append({
            "id": chunk.get("id", f"{chunk['title']}_{chunk['page_labels'][0]}"),
            "values": emb,
            "metadata": {
                "title": chunk["title"],
                "page": chunk["page_labels"][0],
                "text": chunk["text"],
            },
        })
    logger.info(f"🔼 Upserting {len(vectors)} vectors vào Pinecone index={index_name}")
    index.upsert(vectors=vectors, namespace=namespace)
    logger.info("✅ Upsert hoàn tất.")
