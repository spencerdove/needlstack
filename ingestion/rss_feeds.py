"""
RSS feed ingestion using feedparser. Fetches articles from financial news sources,
deduplicates by URL hash, and upserts into news_articles table.
"""
import hashlib
import logging
from datetime import datetime
from typing import Optional

import feedparser
import sqlalchemy as sa

from db.schema import get_engine

logger = logging.getLogger(__name__)

NEWS_SOURCES = [
    {
        "source_id": "reuters_business",
        "name": "Reuters Business",
        "rss_url": "https://feeds.reuters.com/reuters/businessNews",
    },
    {
        "source_id": "marketwatch_pulse",
        "name": "MarketWatch",
        "rss_url": "https://feeds.content.dowjones.io/public/rss/mw_marketpulse",
    },
    {
        "source_id": "yahoo_finance",
        "name": "Yahoo Finance",
        "rss_url": "https://finance.yahoo.com/news/rssindex",
    },
    {
        "source_id": "cnbc_top_news",
        "name": "CNBC",
        "rss_url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
    },
    {
        "source_id": "benzinga",
        "name": "Benzinga",
        "rss_url": "https://www.benzinga.com/feed",
    },
    {
        "source_id": "motley_fool",
        "name": "Motley Fool",
        "rss_url": "https://www.fool.com/feeds/index.aspx",
    },
    {
        "source_id": "federal_reserve",
        "name": "Federal Reserve",
        "rss_url": "https://www.federalreserve.gov/feeds/press_all.xml",
    },
    {
        "source_id": "sec_8k",
        "name": "SEC 8-K",
        "rss_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&output=atom",
    },
]

PAYWALLED_SOURCES = {"wsj", "bloomberg", "ft"}


def seed_news_sources(engine: Optional[sa.Engine] = None) -> None:
    """Upsert all NEWS_SOURCES into the news_sources table."""
    if engine is None:
        engine = get_engine()

    with engine.begin() as conn:
        for source in NEWS_SOURCES:
            conn.execute(
                sa.text(
                    """
                    INSERT OR REPLACE INTO news_sources
                        (source_id, name, rss_url, is_active, fetch_interval_min)
                    VALUES
                        (:source_id, :name, :rss_url, 1, 30)
                    """
                ),
                {
                    "source_id": source["source_id"],
                    "name": source["name"],
                    "rss_url": source["rss_url"],
                },
            )
    logger.info(f"Seeded {len(NEWS_SOURCES)} news sources.")


def _make_article_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:32]


def _parse_published(entry) -> datetime:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            return datetime(*entry.published_parsed[:6])
        except Exception:
            pass
    return datetime.utcnow()


def poll_feed(
    source_id: str,
    rss_url: str,
    engine: Optional[sa.Engine] = None,
) -> tuple[int, int]:
    """
    Fetch and parse a single RSS feed, upsert new articles into news_articles.

    Returns (new_articles, skipped_duplicates).
    """
    if engine is None:
        engine = get_engine()

    is_paywalled = 1 if source_id in PAYWALLED_SOURCES else 0

    try:
        feed = feedparser.parse(rss_url)
    except Exception as exc:
        logger.error(f"[{source_id}] feedparser error: {exc}")
        return 0, 0

    if feed.bozo and not feed.entries:
        logger.warning(f"[{source_id}] Feed parse error (bozo): {feed.bozo_exception}")
        return 0, 0

    new_articles = 0
    skipped = 0
    now = datetime.utcnow()

    for entry in feed.entries:
        url = getattr(entry, "link", None) or getattr(entry, "id", None)
        if not url:
            skipped += 1
            continue

        article_id = _make_article_id(url)
        title = getattr(entry, "title", None)
        author = getattr(entry, "author", None)
        published_at = _parse_published(entry)
        raw_rss_summary = getattr(entry, "summary", None)
        categories = None
        if hasattr(entry, "tags") and entry.tags:
            try:
                categories = ",".join(
                    t.get("term", "") for t in entry.tags if t.get("term")
                )
            except Exception:
                categories = None

        try:
            with engine.begin() as conn:
                # Check for existing article
                existing = conn.execute(
                    sa.text(
                        "SELECT article_id FROM news_articles WHERE article_id = :aid"
                    ),
                    {"aid": article_id},
                ).fetchone()

                if existing:
                    skipped += 1
                    continue

                conn.execute(
                    sa.text(
                        """
                        INSERT OR REPLACE INTO news_articles
                            (article_id, source_id, url, title, author,
                             published_at, fetched_at, raw_rss_summary,
                             is_paywalled, categories)
                        VALUES
                            (:article_id, :source_id, :url, :title, :author,
                             :published_at, :fetched_at, :raw_rss_summary,
                             :is_paywalled, :categories)
                        """
                    ),
                    {
                        "article_id": article_id,
                        "source_id": source_id,
                        "url": url,
                        "title": title,
                        "author": author,
                        "published_at": published_at,
                        "fetched_at": now,
                        "raw_rss_summary": raw_rss_summary,
                        "is_paywalled": is_paywalled,
                        "categories": categories,
                    },
                )
                new_articles += 1
        except Exception as exc:
            logger.error(f"[{source_id}] DB insert error for url={url}: {exc}")
            skipped += 1

    logger.info(
        f"[{source_id}] new={new_articles} skipped={skipped}"
    )
    return new_articles, skipped


def poll_all_feeds(
    engine: Optional[sa.Engine] = None,
) -> tuple[int, list[str]]:
    """
    Poll all active news sources in NEWS_SOURCES.

    Returns (total_new_articles, failed_source_ids).
    Updates last_fetched_at in news_sources after each feed.
    """
    if engine is None:
        engine = get_engine()

    total_new = 0
    failed: list[str] = []

    for source in NEWS_SOURCES:
        source_id = source["source_id"]
        rss_url = source["rss_url"]
        try:
            new, _ = poll_feed(source_id, rss_url, engine=engine)
            total_new += new
        except Exception as exc:
            logger.error(f"[{source_id}] Unhandled error: {exc}")
            failed.append(source_id)
            continue
        finally:
            # Update last_fetched_at regardless of success/failure
            try:
                with engine.begin() as conn:
                    conn.execute(
                        sa.text(
                            """
                            UPDATE news_sources
                            SET last_fetched_at = :ts
                            WHERE source_id = :source_id
                            """
                        ),
                        {"ts": datetime.utcnow(), "source_id": source_id},
                    )
            except Exception as exc2:
                logger.warning(
                    f"[{source_id}] Could not update last_fetched_at: {exc2}"
                )

    logger.info(
        f"poll_all_feeds complete: total_new={total_new} failed={failed}"
    )
    return total_new, failed
