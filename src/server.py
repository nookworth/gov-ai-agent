from typing import Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from src.chatbot import UtahBillAnalyst

load_dotenv()

app = FastAPI(title="Government AI Agent API")
analyst = UtahBillAnalyst()
origins = [
    "http://localhost:5173",
    "https://utah-gov-ai.vercel.app",
    "https://gov-ai-agent.fly.dev",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


class AnalysisRequest(BaseModel):
    bill_id: int
    query: str


class AnalysisResponse(BaseModel):
    analysis: str


class SearchRequest(BaseModel):
    bill_id: int
    query: str
    match_threshold: float = 0.8
    match_count: int = 3


class SearchResponse(BaseModel):
    results: Dict[str, Any]


@app.post("/analyze", response_model=AnalysisResponse)
async def analyze_bill(request: AnalysisRequest):
    """
    Analyze a user query about a Utah bill using AI-powered multi-step reasoning.
    This is the main endpoint that provides intelligent analysis rather than raw search results.
    """
    try:
        analysis = analyst.analyze_query(request.bill_id, request.query)
        return AnalysisResponse(analysis=analysis)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=3000)
