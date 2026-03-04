"""
StockTwits public API ingestion. No auth required (200 req/hr).
API: https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json
"""
import hashlib
import json
import logging
import time
from datetime import datetime
from typing import Optional

import requests
import sqlalchemy as sa

from db.schema import get_engine
from ingestion.ticker_mentions import load_ticker_cache, extract_ticker_mentions
from ingestion.sentiment import score_text, VADER_VERSION

logger = logging.getLogger(__name__)

_STOCKTWITS_API = "https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"


def _make_content_id(stocktwits_id) -> str:
    return hashlib.sha256(f"stocktwits:{stocktwits_id}".encode()).hexdigest()[:32]


def _parse_stocktwits_dt(dt_str: str) -> datetime:
    """Parse StockTwits ISO timestamp into a naive UTC datetime."""
    try:
        # StockTwits returns e.g. "2024-01-15T12:34:56Z"
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return datetime.utcnow()


def poll_stocktwits(
    tickers: list[str],
    engine: Optional[sa.Engine] = None,
    delay: float = 0.5,
) -> tuple[int, list[str]]:
    """
    Poll StockTwits public stream for each ticker.

    Maps messages to content_items with source_type='stocktwits'.
    Upserts content_tickers for the ticker being polled.
    Stores native StockTwits sentiment if present.
    Also runs VADER ticker mentions + sentiment inline on new items.

    Returns (new_items_inserted, failed_tickers).
    """
    if engine is None:
        engine = get_engine()

    ticker_cache = load_ticker_cache(engine)

    new_items_inserted = 0
    failed_tickers: list[str] = []

    for ticker in tickers:
        url = _STOCKTWITS_API.format(ticker=ticker)
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 429:
                logger.warning(
                    f"[stocktwits/{ticker}] Rate limited (429). Stopping early."
                )
                failed_tickers.append(ticker)
                break
            if resp.status_code != 200:
                logger.warning(
                    f"[stocktwits/{ticker}] HTTP {resp.status_code}; skipping."
                )
                failed_tickers.append(ticker)
                time.sleep(delay)
                continue

            data = resp.json()
            messages = data.get("messages", [])
            if not messages:
                time.sleep(delay)
                continue

            now = datetime.utcnow()

            for msg in messages:
                msg_id = msg.get("id")
                if not msg_id:
                    continue

                content_id = _make_content_id(msg_id)

                # Check existence
                with engine.connect() as conn:
                    existing = conn.execute(
                        sa.text(
                            "SELECT content_id FROM content_items WHERE content_id = :cid"
                        ),
                        {"cid": content_id},
                    ).fetchone()

                if existing:
                    continue

                body_text = msg.get("body", "") or ""
                author = None
                user = msg.get("user")
                if user:
                    author = user.get("username")

                created_at_str = msg.get("created_at", "")
                published_at = _parse_stocktwits_dt(created_at_str) if created_at_str else now

                word_count = len(body_text.split()) if body_text else 0
                raw_json = json.dumps(msg)

                # Native StockTwits sentiment
                native_sentiment_label: Optional[str] = None
                try:
                    entities = msg.get("entities", {})
                    native_basic = entities.get("sentiment", {})
                    if native_basic:
                        basic_val = native_basic.get("basic")
                        if basic_val == "Bullish":
                            native_sentiment_label = "bullish"
                        elif basic_val == "Bearish":
                            native_sentiment_label = "bearish"
                except Exception:
                    pass

                try:
                    with engine.begin() as conn:
                        conn.execute(
                            sa.text(
                                """
                                INSERT OR REPLACE INTO content_items
                                    (content_id, source_type, source_id, external_id,
                                     url, title, author, published_at, fetched_at,
                                     body_text, word_count, engagement_score, raw_json)
                                VALUES
                                    (:content_id, :source_type, :source_id, :external_id,
                                     :url, :title, :author, :published_at, :fetched_at,
                                     :body_text, :word_count, :engagement_score, :raw_json)
                                """
                            ),
                            {
                                "content_id": content_id,
                                "source_type": "stocktwits",
                                "source_id": ticker,
                                "external_id": str(msg_id),
                                "url": f"https://stocktwits.com/message/{msg_id}",
                                "title": None,
                                "author": author,
                                "published_at": published_at,
                                "fetched_at": now,
                                "body_text": body_text,
                                "word_count": word_count,
                                "engagement_score": None,
                                "raw_json": raw_json,
                            },
                        )
                    new_items_inserted += 1
                except Exception as exc:
                    logger.error(
                        f"[stocktwits/{ticker}] DB insert error for msg {msg_id}: {exc}"
                    )
                    continue

                # Upsert content_tickers for the polled ticker
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            sa.text(
                                """
                                INSERT OR REPLACE INTO content_tickers
                                    (content_id, ticker, mention_count,
                                     mention_in_title, confidence)
                                VALUES
                                    (:content_id, :ticker, 1, 0, 1.0)
                                """
                            ),
                            {"content_id": content_id, "ticker": ticker},
                        )
                except Exception as exc:
                    logger.error(
                        f"[stocktwits/{ticker}] content_tickers error: {exc}"
                    )

                # Also check for additional ticker mentions in body text
                try:
                    mentions = extract_ticker_mentions(
                        text=body_text,
                        title="",
                        known_tickers=ticker_cache,
                    )
                    extra = [m for m in mentions if m["ticker"] != ticker]
                    if extra:
                        with engine.begin() as conn:
                            conn.execute(
                                sa.text(
                                    """
                                    INSERT OR REPLACE INTO content_tickers
                                        (content_id, ticker, mention_count,
                                         mention_in_title, confidence)
                                    VALUES
                                        (:content_id, :ticker, :mention_count,
                                         :mention_in_title, 1.0)
                                    """
                                ),
                                [
                                    {
                                        "content_id": content_id,
                                        "ticker": m["ticker"],
                                        "mention_count": m["mention_count"],
                                        "mention_in_title": m["mention_in_title"],
                                    }
                                    for m in extra
                                ],
                            )
                except Exception as exc:
                    logger.error(
                        f"[stocktwits/{ticker}] Extra ticker mentions error: {exc}"
                    )

                # Sentiment: prefer native if available, else VADER
                try:
                    if native_sentiment_label:
                        # Use native sentiment but still compute VADER scores
                        sent = score_text(body_text.strip())
                        final_label = native_sentiment_label
                    else:
                        sent = score_text(body_text.strip())
                        final_label = sent["sentiment_label"]

                    with engine.begin() as conn:
                        conn.execute(
                            sa.text(
                                """
                                INSERT OR REPLACE INTO content_sentiment
                                    (content_id, compound_score, positive, negative,
                                     neutral, sentiment_label, scored_at, model_version)
                                VALUES
                                    (:content_id, :compound_score, :positive, :negative,
                                     :neutral, :sentiment_label, :scored_at, :model_version)
                                """
                            ),
                            {
                                "content_id": content_id,
                                "compound_score": sent["compound"],
                                "positive": sent["positive"],
                                "negative": sent["negative"],
                                "neutral": sent["neutral"],
                                "sentiment_label": final_label,
                                "scored_at": now,
                                "model_version": VADER_VERSION,
                            },
                        )
                except Exception as exc:
                    logger.error(
                        f"[stocktwits/{ticker}] Sentiment error for {content_id}: {exc}"
                    )

        except Exception as exc:
            logger.error(f"[stocktwits/{ticker}] Unhandled error: {exc}")
            failed_tickers.append(ticker)

        time.sleep(delay)

    logger.info(
        f"poll_stocktwits: new_items={new_items_inserted} "
        f"failed_tickers={failed_tickers}"
    )
    return new_items_inserted, failed_tickers
