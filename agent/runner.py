"""
Agentic loop using Anthropic Claude API with tool_use.
Stores conversation and messages in DB.
"""
import json
import logging
import os
import uuid
from datetime import datetime
from typing import Optional

import sqlalchemy as sa
from anthropic import Anthropic

from db.schema import get_engine
from agent.tools import TOOL_DEFINITIONS, execute_tool

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"
MAX_ITERATIONS = 10

SYSTEM_PROMPT = """You are an investment research assistant with access to the Needlstack financial data lake.
You have tools to query real market data, financial statements, valuation metrics, institutional ownership, and market sentiment.
Always cite specific data points in your analysis. Be precise with numbers and dates.
When comparing companies, use the compare_tickers tool to get structured data.
For screening, use screen_stocks with specific metric thresholds.
Provide balanced analysis — highlight both opportunities and risks."""


def _get_client() -> Anthropic:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "ANTHROPIC_API_KEY environment variable is not set. "
            "Set it in your .env file or environment before running the agent."
        )
    return Anthropic(api_key=api_key)


def _insert_conversation(
    engine: sa.Engine,
    conversation_id: str,
    context_tickers: Optional[list[str]],
) -> None:
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR IGNORE INTO agent_conversations
                    (conversation_id, created_at, context_tickers, model_used)
                VALUES
                    (:conversation_id, :created_at, :context_tickers, :model_used)
                """
            ),
            {
                "conversation_id": conversation_id,
                "created_at": datetime.utcnow().isoformat(),
                "context_tickers": json.dumps(context_tickers) if context_tickers else None,
                "model_used": MODEL,
            },
        )


def _insert_message(
    engine: sa.Engine,
    conversation_id: str,
    role: str,
    content: any,
    tokens_used: Optional[int] = None,
) -> None:
    content_str = json.dumps(content) if not isinstance(content, str) else content
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT INTO agent_messages
                    (message_id, conversation_id, role, content, created_at, tokens_used)
                VALUES
                    (:message_id, :conversation_id, :role, :content, :created_at, :tokens_used)
                """
            ),
            {
                "message_id": str(uuid.uuid4()),
                "conversation_id": conversation_id,
                "role": role,
                "content": content_str,
                "created_at": datetime.utcnow().isoformat(),
                "tokens_used": tokens_used,
            },
        )


def run_agent(
    user_message: str,
    context_tickers: Optional[list[str]] = None,
    engine: Optional[sa.Engine] = None,
) -> str:
    """
    Run the agentic loop for a user question.

    1. Creates a new conversation record in the DB.
    2. Sends the message to Claude with TOOL_DEFINITIONS.
    3. Executes any tool calls and feeds results back.
    4. Repeats until stop_reason == 'end_turn' or MAX_ITERATIONS reached.
    5. Stores each message turn in agent_messages.
    6. Returns the final assistant text response.
    """
    if engine is None:
        engine = get_engine()

    client = _get_client()

    conversation_id = str(uuid.uuid4())
    _insert_conversation(engine, conversation_id, context_tickers)

    # Build initial system context with tickers if provided
    system_content = SYSTEM_PROMPT
    if context_tickers:
        tickers_str = ", ".join(context_tickers)
        system_content += f"\n\nContext tickers for this conversation: {tickers_str}"

    messages = [{"role": "user", "content": user_message}]
    _insert_message(engine, conversation_id, "user", user_message)

    final_response = ""

    for iteration in range(MAX_ITERATIONS):
        logger.debug(f"Agent iteration {iteration + 1}/{MAX_ITERATIONS}")

        response = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=system_content,
            tools=TOOL_DEFINITIONS,
            messages=messages,
        )

        tokens_used = response.usage.input_tokens + response.usage.output_tokens if response.usage else None

        # Store assistant response content
        _insert_message(
            engine,
            conversation_id,
            "assistant",
            [block.model_dump() for block in response.content],
            tokens_used=tokens_used,
        )

        # Append assistant turn to conversation history
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            # Extract text from the final response
            for block in response.content:
                if hasattr(block, "type") and block.type == "text":
                    final_response = block.text
            break

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if hasattr(block, "type") and block.type == "tool_use":
                    logger.debug(f"Executing tool: {block.name} with input: {block.input}")
                    result = execute_tool(block.name, block.input, engine=engine)
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": json.dumps(result),
                        }
                    )

            if tool_results:
                _insert_message(engine, conversation_id, "user", tool_results)
                messages.append({"role": "user", "content": tool_results})
        else:
            # Unexpected stop reason — extract any text and break
            for block in response.content:
                if hasattr(block, "type") and block.type == "text":
                    final_response = block.text
            logger.warning(f"Unexpected stop_reason: {response.stop_reason}")
            break
    else:
        logger.warning(f"Agent reached max iterations ({MAX_ITERATIONS}) without end_turn")
        # Return whatever text was last generated
        for block in response.content:
            if hasattr(block, "type") and block.type == "text":
                final_response = block.text

    return final_response
