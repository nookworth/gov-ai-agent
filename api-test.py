import json
import os
import requests
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings

load_dotenv()

PUBLIC_KEY = os.getenv("TIDB_DATAAPP_PUBLIC_KEY")
PRIVATE_KEY = os.getenv("TIDB_DATAAPP_PRIVATE_KEY")
URL = "https://us-west-2.data.tidbcloud.com/api/v1beta/app/dataapp-raHlDywv/endpoint/vector_search"

embeddings = OpenAIEmbeddings()
bill_name = input("Enter the bill you'd like to query:\n")
query = input("Enter a query:\n") or "Who was the chief sponsor of the bill?"
embedded_query = embeddings.embed_query(query)

payload = {
    "bill_name": bill_name,
    "query_vector": json.dumps(embedded_query),
    "match_threshold": 0.8,
    "match_count": 3,
}

response = requests.post(
    url=URL,
    auth=(PUBLIC_KEY, PRIVATE_KEY),
    headers={
        "content-type": "application/json",
        "endpoint-type": "draft",
    },
    json=payload,
)

print(response.json())
