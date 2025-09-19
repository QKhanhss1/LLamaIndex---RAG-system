import os
import logging
from dotenv import load_dotenv
from extract import extract_text_with_fallback, extract_tables_from_pdf
from transform import split_chunk_semantic_sentence
from load import upsert_chunks_to_pinecone

load_dotenv()
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
INDEX_NAME = "ragflow"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

# ----------- ETL Pipeline -----------
def pipeline_etl(pdf_path: str, output_csv: str, max_tokens: int = 1024, namespace: str = "default"):
	# Extract
	docs = extract_text_with_fallback(pdf_path)
	tables = extract_tables_from_pdf(pdf_path, output_csv=output_csv)
	if tables:
		docs[0]["tables"] = [tables[0].head().to_dict()]
	# Transform
	chunks = split_chunk_semantic_sentence(docs, max_tokens=max_tokens, openai_api_key=OPENAI_API_KEY)
	# Load
	upsert_chunks_to_pinecone(
		chunks,
		index_name=INDEX_NAME,
		openai_api_key=OPENAI_API_KEY,
		pinecone_api_key=PINECONE_API_KEY,
		namespace=namespace,
	)
	logger.info("✅ Pipeline ETL hoàn tất.")

if __name__ == "__main__":
	# Ví dụ chạy ETL pipeline
	pdf_file = "./data/Brochure- Hưng nghiệp hưu trí.pdf"
	pipeline_etl(pdf_file, output_csv="./output/tables", max_tokens=512)
