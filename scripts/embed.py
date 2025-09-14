import glob
import json
import os
import re
from dotenv import load_dotenv
from langchain_community.document_loaders import TextLoader
from langchain_community.vectorstores import TiDBVectorStore
from langchain_openai import OpenAIEmbeddings
from langchain_text_splitters import CharacterTextSplitter
import mysql.connector

load_dotenv()

CONNECTION = mysql.connector.connect(
  host = "gateway01.us-west-2.prod.aws.tidbcloud.com",
  port = 4000,
  user = "3p2Vbed9kTce2wd.root",
  password = os.getenv("TIDB_CLUSTER0_PASSWORD"),
  database = "gov_ai",
  ssl_ca = "/etc/ssl/cert.pem",
  ssl_verify_cert = True,
  ssl_verify_identity = True
)
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

def setup_available_bills_table():
    try:
        cursor = CONNECTION.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS available_bills (
            bill_id INT PRIMARY KEY,
            bill_number VARCHAR(20) NOT NULL,
            source VARCHAR(255),
            title TEXT,
            INDEX idx_bill_number (bill_number)
        )
        """
        cursor.execute(create_table_query)
        CONNECTION.commit()
        print("Available bills table created or already exists")
        return True
    except Exception as e:
        print(f"Error setting up available_bills table: {e}")
        return False

def add_bill_to_available_bills(bill_id, bill_number, source, title):
    try:
        cursor = CONNECTION.cursor()
        insert_query = """
        INSERT INTO available_bills (bill_id, bill_number, source, title)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            source = VALUES(source),
            title = VALUES(title)
        """
        cursor.execute(insert_query, (bill_id, bill_number, source, title))
        CONNECTION.commit()
    except Exception as e:
        print(f"Error adding bill {bill_number} to available_bills: {e}")

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

    if not setup_available_bills_table():
        print("Failed to setup available_bills table. Exiting.")
        exit(1)

    bills_processed = set()

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

            if bill_number.upper() not in bills_processed and bill_info:
                add_bill_to_available_bills(
                    bill_info.get("bill_id"),
                    bill_number.upper(),
                    os.path.basename(file_path),
                    bill_info.get("title", "")
                )
                bills_processed.add(bill_number.upper())
                print(f"Added {bill_number} to available_bills table")

        except Exception as e:
            print(f"Error reading {os.path.basename(file_path)}: {e}")

    CONNECTION.close()
    print(f"Processing complete. Added {len(bills_processed)} bills to available_bills table.")
