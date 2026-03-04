"""
Reddit ingestion using PRAW. Polls hot + new posts from financial subreddits.
Credentials from .env: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT
"""
import hashlib
import json
import logging
import math
import os
from datetime import datetime, timezone
from typing import Optional

import sqlalchemy as sa
from dotenv import load_dotenv

from db.schema import get_engine
from ingestion.ticker_mentions import load_ticker_cache, extract_ticker_mentions
from ingestion.sentiment import score_text, VADER_VERSION

load_dotenv()

logger = logging.getLogger(__name__)

SUBREDDITS = [
    "wallstreetbets",
    "investing",
    "stocks",
    "options",
    "SecurityAnalysis",
    "ValueInvesting",
]


def _make_content_id(reddit_id: str) -> str:
    return hashlib.sha256(f"reddit:{reddit_id}".encode()).hexdigest()[:32]


def poll_reddit(
    engine: Optional[sa.Engine] = None,
    limit: int = 100,
) -> tuple[int, list[str]]:
    """
    Poll hot + new posts from each subreddit in SUBREDDITS.

    Requires env vars: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT.
    Maps posts to content_items with source_type='reddit'.
    Also runs ticker mentions + sentiment inline on new items.

    Returns (new_items_inserted, failed_subreddits).
    """
    if engine is None:
        engine = get_engine()

    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT", "needlstack/1.0")

    if not client_id or not client_secret:
        logger.warning(
            "REDDIT_CLIENT_ID or REDDIT_CLIENT_SECRET not set; skipping Reddit poll."
        )
        return 0, []

    try:
        import praw
    except ImportError:
        logger.error("praw is not installed. Run: pip install praw")
        return 0, list(SUBREDDITS)

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
    )

    # Load ticker cache once
    ticker_cache = load_ticker_cache(engine)

    new_items_inserted = 0
    failed_subreddits: list[str] = []

    for subreddit_name in SUBREDDITS:
        try:
            subreddit = reddit.subreddit(subreddit_name)
            posts = []

            # hot posts
            try:
                posts.extend(list(subreddit.hot(limit=limit)))
            except Exception as exc:
                logger.warning(f"[r/{subreddit_name}] hot() error: {exc}")

            # new posts (half the limit)
            try:
                posts.extend(list(subreddit.new(limit=limit // 2)))
            except Exception as exc:
                logger.warning(f"[r/{subreddit_name}] new() error: {exc}")

            seen_ids: set[str] = set()
            for post in posts:
                if post.id in seen_ids:
                    continue
                seen_ids.add(post.id)

                content_id = _make_content_id(post.id)

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

                engagement_score = math.log1p(
                    (post.score or 0) + (post.num_comments or 0)
                )
                published_at = datetime.fromtimestamp(
                    post.created_utc, tz=timezone.utc
                ).replace(tzinfo=None)
                body_text = post.selftext or ""
                url = f"https://reddit.com{post.permalink}"
                word_count = len(body_text.split()) if body_text else 0

                raw_json = json.dumps(
                    {
                        "id": post.id,
                        "score": post.score,
                        "num_comments": post.num_comments,
                        "upvote_ratio": post.upvote_ratio,
                        "url": post.url,
                        "is_self": post.is_self,
                    }
                )

                now = datetime.utcnow()

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
                                "source_type": "reddit",
                                "source_id": subreddit_name,
                                "external_id": post.id,
                                "url": url,
                                "title": post.title,
                                "author": str(post.author) if post.author else None,
                                "published_at": published_at,
                                "fetched_at": now,
                                "body_text": body_text,
                                "word_count": word_count,
                                "engagement_score": engagement_score,
                                "raw_json": raw_json,
                            },
                        )
                    new_items_inserted += 1
                except Exception as exc:
                    logger.error(
                        f"[r/{subreddit_name}] DB insert error for post {post.id}: {exc}"
                    )
                    continue

                # Inline ticker mentions
                try:
                    mentions = extract_ticker_mentions(
                        text=body_text,
                        title=post.title or "",
                        known_tickers=ticker_cache,
                    )
                    if mentions:
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
                                    for m in mentions
                                ],
                            )
                except Exception as exc:
                    logger.error(
                        f"[r/{subreddit_name}] Ticker mentions error for {content_id}: {exc}"
                    )

                # Inline sentiment
                try:
                    combined_text = (post.title or "") + " " + body_text
                    sent = score_text(combined_text.strip())
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
                                "sentiment_label": sent["sentiment_label"],
                                "scored_at": now,
                                "model_version": VADER_VERSION,
                            },
                        )
                except Exception as exc:
                    logger.error(
                        f"[r/{subreddit_name}] Sentiment error for {content_id}: {exc}"
                    )

        except Exception as exc:
            logger.error(f"[r/{subreddit_name}] Unhandled error: {exc}")
            failed_subreddits.append(subreddit_name)

    logger.info(
        f"poll_reddit: new_items={new_items_inserted} "
        f"failed_subreddits={failed_subreddits}"
    )
    return new_items_inserted, failed_subreddits
