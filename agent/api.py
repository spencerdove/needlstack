"""
FastAPI REST API for the Needlstack AI agent with SSE streaming.
Deploy at api.needlstack.com.

Usage:
    uvicorn agent.api:app --host 0.0.0.0 --port 8000
"""
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from db.schema import get_engine, init_db
from agent.runner import run_agent
from agent.usage import router as usage_router
from ingestion.feedback import store_feedback

logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(title="Needlstack Agent API")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

allowed_origins = os.getenv(
    "ALLOWED_ORIGINS", "https://needlstack.com,http://localhost:3000"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in allowed_origins],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(usage_router)


class ChatRequest(BaseModel):
    message: str
    tickers: Optional[List[str]] = None
    conversation_id: Optional[str] = None


@app.post("/chat")
@limiter.limit("10/minute")
async def chat(request: Request, chat_request: ChatRequest):
    """Non-streaming chat endpoint. Runs the agent and returns the final response."""
    try:
        engine = get_engine()
        response_text = run_agent(
            user_message=chat_request.message,
            context_tickers=chat_request.tickers,
            engine=engine,
        )
        return {
            "response": response_text,
            "tickers": chat_request.tickers,
        }
    except EnvironmentError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        logger.error(f"Chat error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/chat/stream")
@limiter.limit("10/minute")
async def chat_stream(request: Request, chat_request: ChatRequest):
    """
    Streaming chat endpoint using Server-Sent Events.
    Yields SSE events as the agent produces output.
    """
    async def event_generator():
        try:
            engine = get_engine()
            response_text = run_agent(
                user_message=chat_request.message,
                context_tickers=chat_request.tickers,
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


@app.post("/webhook/email")
async def webhook_email(request: Request):
    """
    Receive inbound email webhooks from Resend.
    Extracts sender, subject, body and stores as feedback.
    """
    try:
        payload = await request.json()
        sender_email = payload.get("from", "")
        sender_name = payload.get("from_name", "")
        subject = payload.get("subject", "")
        body = payload.get("text", "") or payload.get("html", "")

        # Try to extract changelog version from subject (e.g. "Re: Needlstack v4.0 — ...")
        version_match = re.search(r"v\d+\.\d+", subject)
        changelog_version = version_match.group(0) if version_match else None

        store_feedback(
            body=body,
            source="email",
            sender_email=sender_email,
            sender_name=sender_name,
            subject=subject,
            changelog_version=changelog_version,
        )
        return {"status": "ok"}
    except Exception as exc:
        logger.error(f"Webhook error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail="Webhook processing failed")


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("agent.api:app", host="0.0.0.0", port=8000, reload=False)
