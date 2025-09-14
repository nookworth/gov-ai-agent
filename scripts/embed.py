import glob
import json
import os
import re
from dotenv import load_dotenv
from langchain_community.document_loaders import TextLoader
from langchain_community.vectorstores import TiDBVectorStore
from langchain_openai import OpenAIEmbeddings
from langchain_text_splitters import CharacterTextSplitter

load_dotenv()

DIRECTORY_PATH = "data"
PATTERN = os.path.join(DIRECTORY_PATH, "*.txt")
MASTER_LIST_PATH = os.path.join(DIRECTORY_PATH, "2025_general_session_master_list.json")
TIDB_CONNECTION_STRING = os.getenv("TIDB_CONNECTION_STRING")

def load_master_list():
    with open(MASTER_LIST_PATH, 'r') as f:
        data = json.load(f)

    bill_lookup = {}
    for key, bill_info in data['masterlist'].items():
        if key != 'session' and isinstance(bill_info, dict):
            bill_number = bill_info.get('number', '').upper()
            bill_lookup[bill_number] = {
                'bill_id': bill_info.get('bill_id'),
                'title': bill_info.get('title', '')
            }
    return bill_lookup

embeddings = OpenAIEmbeddings()
vector_store = TiDBVectorStore(
    connection_string=TIDB_CONNECTION_STRING,
    distance_strategy="cosine",
    embedding_function=embeddings,
    table_name="embedded_documents",
    drop_existing_table=True,
)

if __name__ == "__main__":
    bill_lookup = load_master_list()

    for file_path in glob.glob(PATTERN):
        try:
            bill_number = re.search(r"[A-Za-z]{2,4}[\d]{4}", file_path).group() or "unknown"
            loader = TextLoader(file_path)
            documents = loader.load()
            text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=10)
            docs = text_splitter.split_documents(documents)

            bill_info = bill_lookup.get(bill_number.upper(), {})

            for doc in docs:
                doc.metadata["bill_number"] = bill_number
                doc.metadata["bill_id"] = bill_info.get("bill_id")
                doc.metadata["title"] = bill_info.get("title", "")

            vector_store.add_documents(docs)
        except Exception as e:
            print(f"Error reading {os.path.basename(file_path)}: {e}")
