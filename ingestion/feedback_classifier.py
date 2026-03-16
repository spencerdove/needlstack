"""
Classify unclassified feedback using Claude.
"""
import json
import logging
import os
from datetime import datetime
from typing import Optional

import sqlalchemy as sa
from anthropic import Anthropic

from db.schema import get_engine, feedback_table

logger = logging.getLogger(__name__)

CLASSIFICATION_PROMPT = """Classify the following user feedback into exactly one category and provide a one-sentence summary.

Categories:
- bug: Reports a problem, error, or unexpected behavior
- feature_request: Suggests new functionality or improvements
- question: Asks for clarification or help
- general: Praise, thanks, or other feedback that doesn't fit above

Feedback:
---
Subject: {subject}
Body: {body}
---

Respond with valid JSON only:
{{"category": "<bug|feature_request|question|general>", "confidence": <0.0-1.0>, "summary": "<one sentence>"}}"""


def classify_feedback(engine: Optional[sa.Engine] = None) -> int:
    """Classify all unclassified feedback rows. Returns count of classified rows."""
    if engine is None:
        engine = get_engine()

    client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    classified = 0

    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(feedback_table).where(feedback_table.c.category.is_(None))
        ).fetchall()

        for row in rows:
            prompt = CLASSIFICATION_PROMPT.format(
                subject=row.subject or "(no subject)",
                body=row.body,
            )

            try:
                resp = client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=200,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = resp.content[0].text.strip()
                result = json.loads(text)

                conn.execute(
                    feedback_table.update()
                    .where(feedback_table.c.id == row.id)
                    .values(
                        category=result["category"],
                        category_confidence=result.get("confidence"),
                        summary=result.get("summary"),
                        classified_at=datetime.utcnow(),
                    )
                )
                conn.commit()
                classified += 1
                logger.info(f"Classified feedback {row.id}: {result['category']}")
            except Exception as exc:
                logger.error(f"Failed to classify feedback {row.id}: {exc}")

    return classified
