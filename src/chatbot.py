import json
import os
import requests
from typing import Dict, Any
from langchain_anthropic import ChatAnthropic
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings

load_dotenv()

PUBLIC_KEY = os.getenv("TIDB_DATAAPP_PUBLIC_KEY")
PRIVATE_KEY = os.getenv("TIDB_DATAAPP_PRIVATE_KEY")
TIDB_URL = "https://us-west-2.data.tidbcloud.com/api/v1beta/app/dataapp-raHlDywv/endpoint/vector_search"

embeddings = OpenAIEmbeddings()

@tool
def search_bill_content(
    bill_id: int, query: str, match_threshold: float = 0.7, match_count: int = 5
) -> Dict[str, Any]:
    """
    Search for content within a specific bill using vector similarity.

    Args:
        bill_id: The bill identifier
        query: The search query or concept to look for
        match_threshold: Similarity threshold (0.0-1.0, lower = more permissive)
        match_count: Number of results to return

    Returns:
        Dictionary containing search results from the bill
    """
    try:
        embedded_query = embeddings.embed_query(query)

        payload = {
            "bill_id": bill_id,
            "query_vector": json.dumps(embedded_query),
            "match_threshold": match_threshold,
            "match_count": match_count,
        }

        response = requests.post(
            url=TIDB_URL,
            auth=(PUBLIC_KEY, PRIVATE_KEY),
            headers={
                "content-type": "application/json",
                "endpoint-type": "draft",
            },
            json=payload,
        )

        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"Search failed with status {response.status_code}"}

    except Exception as e:
        return {"error": f"Search error: {str(e)}"}


class UtahBillAnalyst:
    def __init__(self):
        self.llm = ChatAnthropic(
            model="claude-3-5-haiku-20241022",
            temperature=0.1,
        )
        self.tools = [search_bill_content]
        self.llm_with_tools = self.llm.bind_tools(self.tools)

    def analyze_query(self, bill_id: int, user_query: str) -> str:
        """
        Analyze a user query about a Utah bill using multi-step reasoning and tool calling.
        """
        system_prompt = """You are an expert analyst of Utah government legislation. Your job is to help users understand bills by:

1. **Query Planning**: Break down complex questions into searchable components
2. **Iterative Search**: Use the search_bill_content tool multiple times with different queries to gather comprehensive information
3. **Analysis**: Synthesize findings to answer the user's question thoroughly
4. **Clarity**: Provide clear, well-reasoned responses with specific references to bill content

For complex questions, search multiple times using different keywords and concepts. For example:
- Constitutional questions: search for "constitutional", "amendment", "rights", specific constitutional concepts
- Policy analysis: search for key policy terms, implementation details, exceptions
- Stakeholder impact: search for affected parties, enforcement mechanisms, penalties

Always cite specific sections or content from your search results."""

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(
                content=f"Please analyze this question about bill {bill_id}: {user_query}"
            ),
        ]

        response = self.llm_with_tools.invoke(messages)
        messages.append(response)

        while response.tool_calls:
            for tool_call in response.tool_calls:
                if tool_call["name"] == "search_bill_content":
                    args = tool_call["args"]
                    search_result = search_bill_content.invoke(args)

                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call["id"],
                            "content": json.dumps(search_result, indent=2),
                        }
                    )

            response = self.llm_with_tools.invoke(messages)
            messages.append(response)

        return response.content
