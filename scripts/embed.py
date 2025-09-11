import glob
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
TIDB_CONNECTION_STRING = os.getenv("TIDB_CONNECTION_STRING")

embeddings = OpenAIEmbeddings()
vector_store = TiDBVectorStore(
    connection_string=TIDB_CONNECTION_STRING,
    distance_strategy="cosine",
    embedding_function=embeddings,
    table_name="embedded_documents",
    drop_existing_table=True,
)

if __name__ == "__main__":
    for file_path in glob.glob(PATTERN):
        try:
            bill_id = re.search(r"[A-Za-z]{2,4}[\d]{4}", file_path).group() or "unknown"
            loader = TextLoader(file_path)
            documents = loader.load()
            text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=10)
            docs = text_splitter.split_documents(documents)

            for doc in docs:
                doc.metadata["bill_id"] = bill_id

            vector_store.add_documents(docs)
        except Exception as e:
            print(f"Error reading {os.path.basename(file_path)}: {e}")
