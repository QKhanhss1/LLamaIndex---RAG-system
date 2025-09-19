import os
import logging
import glob
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

def get_all_pdf_files_in_data():
	"""Lấy tất cả file PDF trong thư mục data"""
	data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
	pdf_files = glob.glob(os.path.join(data_dir, "*.pdf"))
	logger.info(f"📁 Tìm thấy {len(pdf_files)} file PDF trong thư mục data")
	return pdf_files

# ----------- ETL Pipeline -----------
def pipeline_etl(pdf_paths: list = None, output_csv: str = "./output/tables", max_tokens: int = 1024, namespace: str = "default"):
	"""
	ETL Pipeline xử lý nhiều PDF files
	Args:
		pdf_paths: Danh sách đường dẫn PDF files. Nếu None, sẽ lấy tất cả PDF trong data/
		output_csv: Đường dẫn output CSV
		max_tokens: Số token tối đa cho mỗi chunk
		namespace: Namespace trong Pinecone
	"""
	# Nếu không có pdf_paths, lấy tất cả PDF trong data/
	if pdf_paths is None:
		pdf_paths = get_all_pdf_files_in_data()
	
	if not pdf_paths:
		logger.warning("⚠️ Không tìm thấy file PDF nào để xử lý!")
		return
	
	all_chunks = []
	
	for pdf_path in pdf_paths:
		logger.info(f"🔄 Xử lý file: {pdf_path}")
		try:
			# Extract text với output file
			pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
			text_output = f"./output/{pdf_name}_extracted_text.txt"
			docs = extract_text_with_fallback(pdf_path, output_txt=text_output)
			
			# Extract tables
			tables = extract_tables_from_pdf(pdf_path, output_csv=f"{output_csv}_{os.path.basename(pdf_path)}")
			if tables:
				docs[0]["tables"] = [tables[0].head().to_dict()]
			
			# Transform
			chunks = split_chunk_semantic_sentence(docs, max_tokens=max_tokens, openai_api_key=OPENAI_API_KEY)
			
			# Thêm metadata file cho mỗi chunk
			for chunk in chunks:
				chunk["source_file"] = os.path.basename(pdf_path)
			
			all_chunks.extend(chunks)
			logger.info(f"✅ Xử lý xong {pdf_path}: {len(chunks)} chunks")
			
		except Exception as e:
			logger.error(f"❌ Lỗi xử lý file {pdf_path}: {e}")
	
	# Load tất cả chunks vào Pinecone
	if all_chunks:
		logger.info(f"🔼 Đang upload {len(all_chunks)} chunks vào Pinecone...")
		upsert_chunks_to_pinecone(
			all_chunks,
			index_name=INDEX_NAME,
			openai_api_key=OPENAI_API_KEY,
			pinecone_api_key=PINECONE_API_KEY,
			namespace=namespace,
		)
		logger.info("✅ Pipeline ETL hoàn tất.")
	else:
		logger.warning("⚠️ Không có chunks nào để upload!")

if __name__ == "__main__":
	# Chạy ETL với tất cả PDF có trong thư mục data/
	logger.info("🚀 Bắt đầu ETL Pipeline với tất cả PDF trong data/")
	
	# Tự động phát hiện và xử lý tất cả file PDF trong data/
	pdf_files = get_all_pdf_files_in_data()
	
	if pdf_files:
		logger.info(f"📁 Tìm thấy {len(pdf_files)} file PDF:")
		for pdf_file in pdf_files:
			logger.info(f"   - {os.path.basename(pdf_file)}")
		
		pipeline_etl(pdf_paths=pdf_files, output_csv="./output/tables", max_tokens=512)
	else:
		logger.warning("⚠️ Không tìm thấy file PDF nào trong thư mục data/")
		logger.info("💡 Hãy đặt file PDF vào thư mục data/ để bắt đầu xử lý")