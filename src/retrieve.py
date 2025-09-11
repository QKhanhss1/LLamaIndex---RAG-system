# src/retrieve.py
import logging
import os
from typing import List

import pinecone
import cohere
from dotenv import load_dotenv

from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.vector_stores.pinecone import PineconeVectorStore
from llama_index.core import StorageContext, VectorStoreIndex
from llama_index.core.schema import NodeWithScore
from llama_index.core.retrievers import VectorIndexRetriever, QueryFusionRetriever

# Load ENV
load_dotenv()
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
INDEX_NAME = os.getenv("PINECONE_INDEX_NAME")
COHERE_API_KEY = os.getenv("COHERE_API_KEY")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)


def get_index() -> VectorStoreIndex:
    """Kết nối tới Pinecone và trả về VectorStoreIndex"""
    pc = pinecone.Pinecone(api_key=PINECONE_API_KEY)
    pinecone_index = pc.Index(INDEX_NAME)

    embed_model = OpenAIEmbedding(model="text-embedding-3-small")

    vector_store = PineconeVectorStore(pinecone_index=pinecone_index,namespace="default")
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    index = VectorStoreIndex.from_vector_store(
        vector_store=vector_store,
        storage_context=storage_context,
        embed_model=embed_model
        
    )
    return index


def cohere_rerank(query: str, nodes: List[NodeWithScore], top_k: int = 5) -> List[NodeWithScore]:
    """Rerank kết quả retrieval bằng Cohere"""
    if not nodes:
        return []

    co = cohere.Client(api_key=COHERE_API_KEY)
    docs = [n.node.get_content() for n in nodes]

    results = co.rerank(
        query=query,
        documents=docs,
        top_n=top_k,
        model="rerank-multilingual-v3.0"
    )

    # Dựng lại list NodeWithScore theo thứ tự rerank
    reranked_nodes: List[NodeWithScore] = []
    for r in results.results:
        idx = r.index
        reranked_nodes.append(
            NodeWithScore(
                node=nodes[idx].node,
                score=r.relevance_score  # lấy điểm của Cohere làm score
            )
        )

    return reranked_nodes



def multiquery_retrieve(query: str, similarity_top_k: int = 10, rerank_top_k: int = 5) -> List[NodeWithScore]:
    """
    Retrieve bằng multi-query retriever (dense) + Cohere rerank
    """
    index = get_index()

    # Dense retriever
    dense_retriever = VectorIndexRetriever(index=index, similarity_top_k=similarity_top_k)

    # Multi-query retriever (tạo paraphrase queries bằng LLM mặc định trong LlamaIndex)
    multiquery_retriever = QueryFusionRetriever(
        [dense_retriever],
        retriever_weights=[1.0],
        num_queries=3,   # số paraphrase query sinh ra
        similarity_top_k=similarity_top_k,
        use_async=False
    )

    logger.info("👉 Đang retrieve dữ liệu (Multi-query dense)...")
    candidate_nodes = multiquery_retriever.retrieve(query)
    logger.info(f"✅ Lấy được {len(candidate_nodes)} candidates từ Pinecone.")

    # Rerank bằng Cohere
    top_nodes = cohere_rerank(query, candidate_nodes, top_k=rerank_top_k)
    logger.info(f"✅ Sau rerank giữ lại {len(top_nodes)} nodes liên quan nhất.")

    return top_nodes


if __name__ == "__main__":
    q = "Thời gian hoạt động của Văn Phòng Đại Diện?"
    results = multiquery_retrieve(q, similarity_top_k=10, rerank_top_k=5)

    print("📌 Query:", q)
    for i, n in enumerate(results, 1):
        print(f"--- Top {i} ---")
        print(n.node.get_content()[:300], "...")
