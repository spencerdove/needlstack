"""
Full text extraction for non-paywalled articles using trafilatura.
Processes articles where full_text IS NULL AND is_paywalled = 0.
"""
import logging
import time
from typing import Optional

import trafilatura
import sqlalchemy as sa

from db.schema import get_engine

logger = logging.getLogger(__name__)


def extract_article_texts(
    engine: Optional[sa.Engine] = None,
    batch_size: int = 50,
) -> tuple[int, int]:
    """
    Fetch full text for articles that have not yet been extracted.

    Queries news_articles WHERE full_text IS NULL AND is_paywalled = 0,
    attempts trafilatura extraction, and updates the record.

    Returns (extracted_count, paywalled_detected).
    """
    if engine is None:
        engine = get_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                """
                SELECT article_id, url
                FROM news_articles
                WHERE full_text IS NULL
                  AND is_paywalled = 0
                LIMIT :batch_size
                """
            ),
            {"batch_size": batch_size},
        ).fetchall()

    if not rows:
        logger.info("No articles pending full-text extraction.")
        return 0, 0

    extracted_count = 0
    paywalled_detected = 0

    for row in rows:
        article_id = row[0]
        url = row[1]

        try:
            downloaded = trafilatura.fetch_url(url)
            result = trafilatura.extract(downloaded) if downloaded else None

            if result is None or len(result.split()) < 100:
                # Treat as paywalled / extraction failed
                with engine.begin() as conn:
                    conn.execute(
                        sa.text(
                            """
                            UPDATE news_articles
                            SET is_paywalled = 1
                            WHERE article_id = :article_id
                            """
                        ),
                        {"article_id": article_id},
                    )
                paywalled_detected += 1
                logger.debug(
                    f"[{article_id}] Extraction yielded no usable text; "
                    "marked paywalled."
                )
            else:
                word_count = len(result.split())
                with engine.begin() as conn:
                    conn.execute(
                        sa.text(
                            """
                            UPDATE news_articles
                            SET full_text = :full_text,
                                word_count = :word_count
                            WHERE article_id = :article_id
                            """
                        ),
                        {
                            "full_text": result,
                            "word_count": word_count,
                            "article_id": article_id,
                        },
                    )
                extracted_count += 1
                logger.debug(
                    f"[{article_id}] Extracted {word_count} words from {url}"
                )

        except Exception as exc:
            logger.error(f"[{article_id}] Extraction error for {url}: {exc}")

        time.sleep(0.5)

    logger.info(
        f"extract_article_texts: extracted={extracted_count} "
        f"paywalled_detected={paywalled_detected}"
    )
    return extracted_count, paywalled_detected
