"""
VADER sentiment scoring for news articles and content items.
"""
import logging
from datetime import datetime
from typing import Optional

import sqlalchemy as sa
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from db.schema import get_engine

logger = logging.getLogger(__name__)

# Module-level analyzer (expensive to initialize repeatedly)
_analyzer = SentimentIntensityAnalyzer()

VADER_VERSION = "vader-3.3.2"


def score_text(text: str) -> dict:
    """
    Score text using VADER and return sentiment dict.

    Returns:
        {
            "compound": float,
            "positive": float,
            "negative": float,
            "neutral": float,
            "sentiment_label": str,  # 'bullish', 'bearish', or 'neutral'
        }
    """
    if not text or not text.strip():
        return {
            "compound": 0.0,
            "positive": 0.0,
            "negative": 0.0,
            "neutral": 1.0,
            "sentiment_label": "neutral",
        }

    scores = _analyzer.polarity_scores(text)
    compound = scores["compound"]

    if compound >= 0.05:
        label = "bullish"
    elif compound <= -0.05:
        label = "bearish"
    else:
        label = "neutral"

    return {
        "compound": compound,
        "positive": scores["pos"],
        "negative": scores["neg"],
        "neutral": scores["neu"],
        "sentiment_label": label,
    }


def score_pending_articles(
    engine: Optional[sa.Engine] = None,
    batch_size: int = 500,
) -> int:
    """
    Score news articles that do not yet have a sentiment record.

    Queries news_articles where article_id NOT IN article_sentiment,
    scores title + full_text (or raw_rss_summary), upserts into article_sentiment.

    Returns count of articles scored.
    """
    if engine is None:
        engine = get_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                """
                SELECT article_id, title, raw_rss_summary, full_text
                FROM news_articles
                WHERE article_id NOT IN (
                    SELECT article_id FROM article_sentiment
                )
                LIMIT :batch_size
                """
            ),
            {"batch_size": batch_size},
        ).fetchall()

    if not rows:
        logger.info("No articles pending sentiment scoring.")
        return 0

    now = datetime.utcnow()
    scored = 0

    records = []
    for row in rows:
        article_id, title, raw_summary, full_text = row
        text_to_score = (title or "") + " " + (
            full_text if full_text else (raw_summary or "")
        )
        result = score_text(text_to_score.strip())
        records.append(
            {
                "article_id": article_id,
                "compound_score": result["compound"],
                "positive": result["positive"],
                "negative": result["negative"],
                "neutral": result["neutral"],
                "sentiment_label": result["sentiment_label"],
                "scored_at": now,
                "model_version": VADER_VERSION,
            }
        )

    try:
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    """
                    INSERT OR REPLACE INTO article_sentiment
                        (article_id, compound_score, positive, negative, neutral,
                         sentiment_label, scored_at, model_version)
                    VALUES
                        (:article_id, :compound_score, :positive, :negative, :neutral,
                         :sentiment_label, :scored_at, :model_version)
                    """
                ),
                records,
            )
        scored = len(records)
    except Exception as exc:
        logger.error(f"Error batch-inserting article sentiment: {exc}")

    logger.info(f"score_pending_articles: scored={scored}")
    return scored


def score_pending_content(
    engine: Optional[sa.Engine] = None,
    batch_size: int = 500,
) -> int:
    """
    Score content_items that do not yet have a sentiment record.

    Queries content_items where content_id NOT IN content_sentiment,
    scores title + body_text, upserts into content_sentiment.

    Returns count of items scored.
    """
    if engine is None:
        engine = get_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                """
                SELECT content_id, title, body_text
                FROM content_items
                WHERE content_id NOT IN (
                    SELECT content_id FROM content_sentiment
                )
                LIMIT :batch_size
                """
            ),
            {"batch_size": batch_size},
        ).fetchall()

    if not rows:
        logger.info("No content items pending sentiment scoring.")
        return 0

    now = datetime.utcnow()
    scored = 0

    records = []
    for row in rows:
        content_id, title, body_text = row
        text_to_score = (title or "") + " " + (body_text or "")
        result = score_text(text_to_score.strip())
        records.append(
            {
                "content_id": content_id,
                "compound_score": result["compound"],
                "positive": result["positive"],
                "negative": result["negative"],
                "neutral": result["neutral"],
                "sentiment_label": result["sentiment_label"],
                "scored_at": now,
                "model_version": VADER_VERSION,
            }
        )

    try:
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    """
                    INSERT OR REPLACE INTO content_sentiment
                        (content_id, compound_score, positive, negative, neutral,
                         sentiment_label, scored_at, model_version)
                    VALUES
                        (:content_id, :compound_score, :positive, :negative, :neutral,
                         :sentiment_label, :scored_at, :model_version)
                    """
                ),
                records,
            )
        scored = len(records)
    except Exception as exc:
        logger.error(f"Error batch-inserting content sentiment: {exc}")

    logger.info(f"score_pending_content: scored={scored}")
    return scored
