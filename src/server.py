import json
import os
import requests
from typing import Dict, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings

load_dotenv()

app = FastAPI(title="Government AI Agent API")

PUBLIC_KEY = os.getenv("TIDB_DATAAPP_PUBLIC_KEY")
PRIVATE_KEY = os.getenv("TIDB_DATAAPP_PRIVATE_KEY")
TIDB_URL = "https://us-west-2.data.tidbcloud.com/api/v1beta/app/dataapp-raHlDywv/endpoint/vector_search"

embeddings = OpenAIEmbeddings()

class SearchRequest(BaseModel):
    bill_id: str
    query: str
    match_threshold: float = 0.8
    match_count: int = 3

class SearchResponse(BaseModel):
    results: Dict[str, Any]

@app.post("/search", response_model=SearchResponse)
async def search_query(request: SearchRequest):
    try:
        embedded_query = embeddings.embed_query(request.query)
        
        payload = {
            "bill_id": request.bill_id,
            "query_vector": json.dumps(embedded_query),
            "match_threshold": request.match_threshold,
            "match_count": request.match_count,
        }
        
        response = requests.post(
            url=TIDB_URL,
            auth=(PUBLIC_KEY, PRIVATE_KEY),
            headers={
                "content-type": "application/json",
                # TODO: may need to change endpoint-type in production
                "endpoint-type": "draft",
            },
            json=payload,
        )
        
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail="TiDB request failed")
        
        return SearchResponse(results=response.json())
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)