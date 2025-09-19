from typing import List, Dict, Any, Optional
from llama_index.core import Document as LlamaDocument
from llama_index.core.node_parser import SentenceSplitter, SemanticSplitterNodeParser
from llama_index.embeddings.openai import OpenAIEmbedding
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

# ----------- Transform -----------
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
                    final_chunks.append({
                        "title": c.get("title"),
                        "text": n.get_content(),
                        "page_labels": c.get("page_labels"),
                        "tables": c.get("tables"),
                    })
                continue
            except Exception as e:
                logger.warning(f"⚠️ SemanticSplitter error: {e}, fallback to SentenceSplitter")
        splitter = SentenceSplitter(
            chunk_size=max_tokens, chunk_overlap=int(max_tokens * 0.1)
        )
        parts = splitter.split_text(text)
        for p in parts:
            final_chunks.append({
                "title": c.get("title"),
                "text": p,
                "page_labels": c.get("page_labels"),
                "tables": c.get("tables"),
            })
    logger.info(f"After splitting: {len(final_chunks)} chunks")
    return final_chunks
