"""
SQL aggregation: compute ticker_sentiment_daily from article_tickers + article_sentiment.
"""
import logging
from datetime import date, timedelta
from typing import Optional

import sqlalchemy as sa

from db.schema import get_engine

logger = logging.getLogger(__name__)


def aggregate_sentiment(
    target_date=None,
    engine: Optional[sa.Engine] = None,
) -> int:
    """
    Aggregate per-ticker sentiment stats for a given date into ticker_sentiment_daily.

    If target_date is None, defaults to yesterday (date.today() - timedelta(days=1)).
    Returns count of ticker rows upserted.
    """
    if engine is None:
        engine = get_engine()

    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    # Normalize to ISO string for SQLite DATE() comparison
    if hasattr(target_date, "isoformat"):
        target_date_str = target_date.isoformat()
    else:
        target_date_str = str(target_date)

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                """
                SELECT
                    at.ticker,
                    COUNT(DISTINCT na.article_id)  AS article_count,
                    COUNT(DISTINCT na.source_id)   AS source_count,
                    SUM(at.mention_count)          AS mention_count,
                    AVG(asent.compound_score)      AS avg_sentiment,
                    SUM(CASE WHEN asent.sentiment_label = 'bullish'
                             THEN 1 ELSE 0 END)    AS bullish_count,
                    SUM(CASE WHEN asent.sentiment_label = 'bearish'
                             THEN 1 ELSE 0 END)    AS bearish_count,
                    SUM(CASE WHEN asent.sentiment_label = 'neutral'
                             THEN 1 ELSE 0 END)    AS neutral_count,
                    SUM(at.mention_in_title)       AS title_mention_count
                FROM article_tickers at
                JOIN news_articles na
                    ON at.article_id = na.article_id
                JOIN article_sentiment asent
                    ON at.article_id = asent.article_id
                WHERE DATE(na.published_at) = :target_date
                GROUP BY at.ticker
                """
            ),
            {"target_date": target_date_str},
        ).fetchall()

    if not rows:
        logger.info(
            f"aggregate_sentiment: no data for {target_date_str}"
        )
        return 0

    upsert_rows = [
        {
            "ticker": row[0],
            "date": target_date_str,
            "article_count": row[1],
            "source_count": row[2],
            "mention_count": row[3],
            "avg_sentiment": row[4],
            "bullish_count": row[5],
            "bearish_count": row[6],
            "neutral_count": row[7],
            "title_mention_count": row[8],
        }
        for row in rows
    ]

    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO ticker_sentiment_daily
                    (ticker, date, article_count, source_count, mention_count,
                     avg_sentiment, bullish_count, bearish_count, neutral_count,
                     title_mention_count)
                VALUES
                    (:ticker, :date, :article_count, :source_count, :mention_count,
                     :avg_sentiment, :bullish_count, :bearish_count, :neutral_count,
                     :title_mention_count)
                """
            ),
            upsert_rows,
        )

    logger.info(
        f"aggregate_sentiment: upserted {len(upsert_rows)} ticker rows "
        f"for {target_date_str}"
    )
    return len(upsert_rows)
