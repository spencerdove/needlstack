"""
FastAPI REST API for the Needlstack AI agent with SSE streaming.
Deploy at api.needlstack.com.

Usage:
    uvicorn agent.api:app --host 0.0.0.0 --port 8000
"""
import json
import logging
import sys
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from db.schema import get_engine, init_db
from agent.runner import run_agent

logger = logging.getLogger(__name__)

app = FastAPI(title="Needlstack Agent API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://needlstack.com", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    message: str
    tickers: Optional[List[str]] = None
    conversation_id: Optional[str] = None


@app.post("/chat")
async def chat(request: ChatRequest):
    """Non-streaming chat endpoint. Runs the agent and returns the final response."""
    try:
        engine = get_engine()
        response_text = run_agent(
            user_message=request.message,
            context_tickers=request.tickers,
            engine=engine,
        )
        return {
            "response": response_text,
            "tickers": request.tickers,
        }
    except EnvironmentError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        logger.error(f"Chat error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/chat/stream")
async def chat_stream(request: ChatRequest):
    """
    Streaming chat endpoint using Server-Sent Events.
    Yields SSE events as the agent produces output.
    """
    async def event_generator():
        try:
            engine = get_engine()
            response_text = run_agent(
                user_message=request.message,
                context_tickers=request.tickers,
                engine=engine,
            )
            # Stream the response in chunks
            chunk_size = 100
            for i in range(0, len(response_text), chunk_size):
                chunk = response_text[i: i + chunk_size]
                yield f"data: {json.dumps({'delta': chunk})}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
        except EnvironmentError as exc:
            yield f"data: {json.dumps({'error': str(exc)})}\n\n"
        except Exception as exc:
            logger.error(f"Stream error: {exc}", exc_info=True)
            yield f"data: {json.dumps({'error': 'Internal server error'})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("agent.api:app", host="0.0.0.0", port=8000, reload=False)
