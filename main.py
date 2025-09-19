

import logging
import os
from dotenv import load_dotenv
import pinecone
import cohere
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.vector_stores.pinecone import PineconeVectorStore
from llama_index.core import StorageContext, VectorStoreIndex
from llama_index.core.schema import NodeWithScore
from llama_index.core.retrievers import VectorIndexRetriever, QueryFusionRetriever
from llama_index.llms.openai import OpenAI


load_dotenv()
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
INDEX_NAME = os.getenv("PINECONE_INDEX_NAME")
COHERE_API_KEY = os.getenv("COHERE_API_KEY")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)

def get_index() -> VectorStoreIndex:
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

def cohere_rerank(query: str, nodes: list, top_k: int = 5) -> list:
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
    reranked_nodes = []
    for r in results.results:
        idx = r.index
        reranked_nodes.append(
            NodeWithScore(
                node=nodes[idx].node,
                score=r.relevance_score
            )
        )
    return reranked_nodes

def multiquery_retrieve(query: str, similarity_top_k: int = 10, rerank_top_k: int = 5) -> list:
    index = get_index()
    dense_retriever = VectorIndexRetriever(index=index, similarity_top_k=similarity_top_k)
    multiquery_retriever = QueryFusionRetriever(
        [dense_retriever],
        retriever_weights=[1.0],
        num_queries=3,
        similarity_top_k=similarity_top_k,
        use_async=False
    )
    logger.info("👉 Đang retrieve dữ liệu (Multi-query dense)...")
    candidate_nodes = multiquery_retriever.retrieve(query)
    logger.info(f"✅ Lấy được {len(candidate_nodes)} candidates từ Pinecone.")
    top_nodes = cohere_rerank(query, candidate_nodes, top_k=rerank_top_k)
    logger.info(f"✅ Sau rerank giữ lại {len(top_nodes)} nodes liên quan nhất.")
    return top_nodes

def rag_agent_answer(query: str, top_nodes: list) -> str:
    """
    Sử dụng LLM để tổng hợp câu trả lời cuối cùng từ các top-k nodes.
    """
    llm = OpenAI(model="gpt-4.1-mini", api_key=os.getenv("OPENAI_API_KEY"))
    context = "\n\n".join([n.node.get_content() for n in top_nodes])
    prompt = f"Dựa trên các đoạn sau, hãy trả lời câu hỏi: '{query}'\n\n{context}"
    response = llm.complete(prompt)
    return response

if __name__ == "__main__":
    while True:
        user_query = input("\nNhập câu hỏi (gõ 'exit' để thoát): ")
        if user_query.strip().lower() == "exit":
            print("Kết thúc phiên hỏi đáp.")
            break
        results = multiquery_retrieve(user_query, similarity_top_k=10, rerank_top_k=5)
        print(f"\n📌 Query: {user_query}")
        for i, n in enumerate(results, 1):
            print(f"--- Top {i} ---")
            print(n.node.get_content()[:300], "...")
        # AI Agent RAG tổng hợp câu trả lời cuối cùng
        final_answer = rag_agent_answer(user_query, results)
        print("\n🔎 Câu trả lời tổng hợp bởi AI Agent:")
        print(final_answer)
