"""
Keyword-phrase based narrative detection. Matches phrases against content_items.
Seed narratives loaded from data/seed_narratives.json.
"""
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import sqlalchemy as sa

from db.schema import get_engine

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_SEED_FILE = _DATA_DIR / "seed_narratives.json"


def load_seed_narratives(engine: Optional[sa.Engine] = None) -> int:
    """
    Read data/seed_narratives.json and upsert all entries into the narratives table.

    Returns count of narratives upserted.
    """
    if engine is None:
        engine = get_engine()

    if not _SEED_FILE.exists():
        logger.error(f"Seed narratives file not found: {_SEED_FILE}")
        return 0

    with open(_SEED_FILE, "r", encoding="utf-8") as f:
        narratives = json.load(f)

    now = datetime.utcnow()
    count = 0

    with engine.begin() as conn:
        for n in narratives:
            keywords_json = json.dumps(n.get("keywords", []))
            related_json = json.dumps(n.get("related_tickers", []))
            conn.execute(
                sa.text(
                    """
                    INSERT OR REPLACE INTO narratives
                        (narrative_id, name, description, keywords,
                         related_tickers, created_at, is_active)
                    VALUES
                        (:narrative_id, :name, :description, :keywords,
                         :related_tickers, :created_at, 1)
                    """
                ),
                {
                    "narrative_id": n["narrative_id"],
                    "name": n["name"],
                    "description": n.get("description", ""),
                    "keywords": keywords_json,
                    "related_tickers": related_json,
                    "created_at": now,
                },
            )
            count += 1

    logger.info(f"load_seed_narratives: upserted {count} narratives.")
    return count


def compute_narrative_signals(
    target_date=None,
    engine: Optional[sa.Engine] = None,
) -> int:
    """
    Compute narrative_signals for all active narratives for a given date.

    If target_date is None, defaults to yesterday.
    Counts content_items whose body_text contains any keyword phrase (case-insensitive).
    Computes momentum_score = mention_count / max(1, prior_7_day_avg) - 1.0.
    Updates narratives.last_seen_at if mention_count > 0.

    Returns count of narrative_signals rows upserted.
    """
    if engine is None:
        engine = get_engine()

    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    if hasattr(target_date, "isoformat"):
        target_date_str = target_date.isoformat()
    else:
        target_date_str = str(target_date)

    # Compute prior 7-day window for momentum baseline
    if hasattr(target_date, "isoformat"):
        prior_start = target_date - timedelta(days=8)
        prior_end = target_date - timedelta(days=1)
    else:
        from datetime import date as _date
        _td = _date.fromisoformat(target_date_str)
        prior_start = _td - timedelta(days=8)
        prior_end = _td - timedelta(days=1)

    prior_start_str = prior_start.isoformat()
    prior_end_str = prior_end.isoformat()

    # Load all active narratives
    with engine.connect() as conn:
        narrative_rows = conn.execute(
            sa.text(
                """
                SELECT narrative_id, name, keywords
                FROM narratives
                WHERE is_active = 1
                """
            )
        ).fetchall()

    if not narrative_rows:
        logger.info("compute_narrative_signals: no active narratives found.")
        return 0

    # Load content items for target date
    with engine.connect() as conn:
        content_rows = conn.execute(
            sa.text(
                """
                SELECT content_id, body_text
                FROM content_items
                WHERE DATE(published_at) = :target_date
                """
            ),
            {"target_date": target_date_str},
        ).fetchall()

    upserted = 0
    now = datetime.utcnow()

    for narrative_id, name, keywords_json in narrative_rows:
        try:
            keywords: list[str] = json.loads(keywords_json) if keywords_json else []
        except Exception:
            keywords = []

        if not keywords:
            continue

        keywords_lower = [kw.lower() for kw in keywords]

        # Count matching content items for target date
        mention_count = 0
        for _cid, body_text in content_rows:
            if not body_text:
                continue
            body_lower = body_text.lower()
            if any(kw in body_lower for kw in keywords_lower):
                mention_count += 1

        # Compute prior 7-day average from narrative_signals table
        with engine.connect() as conn:
            prior_rows = conn.execute(
                sa.text(
                    """
                    SELECT mention_count
                    FROM narrative_signals
                    WHERE narrative_id = :nid
                      AND date >= :prior_start
                      AND date <= :prior_end
                    """
                ),
                {
                    "nid": narrative_id,
                    "prior_start": prior_start_str,
                    "prior_end": prior_end_str,
                },
            ).fetchall()

        if prior_rows:
            prior_avg = sum(r[0] or 0 for r in prior_rows) / len(prior_rows)
        else:
            prior_avg = 0.0

        momentum_score = float(mention_count) / max(1.0, prior_avg) - 1.0

        try:
            with engine.begin() as conn:
                conn.execute(
                    sa.text(
                        """
                        INSERT OR REPLACE INTO narrative_signals
                            (narrative_id, date, mention_count, momentum_score)
                        VALUES
                            (:narrative_id, :date, :mention_count, :momentum_score)
                        """
                    ),
                    {
                        "narrative_id": narrative_id,
                        "date": target_date_str,
                        "mention_count": mention_count,
                        "momentum_score": momentum_score,
                    },
                )
            upserted += 1
        except Exception as exc:
            logger.error(
                f"[narrative/{narrative_id}] Error inserting signal: {exc}"
            )
            continue

        # Update last_seen_at if there were mentions
        if mention_count > 0:
            try:
                with engine.begin() as conn:
                    conn.execute(
                        sa.text(
                            """
                            UPDATE narratives
                            SET last_seen_at = :ts
                            WHERE narrative_id = :nid
                            """
                        ),
                        {"ts": now, "nid": narrative_id},
                    )
            except Exception as exc:
                logger.warning(
                    f"[narrative/{narrative_id}] last_seen_at update error: {exc}"
                )

        logger.debug(
            f"[narrative/{narrative_id}] '{name}': mentions={mention_count} "
            f"momentum={momentum_score:.3f}"
        )

    logger.info(
        f"compute_narrative_signals: upserted {upserted} signals "
        f"for {target_date_str}"
    )
    return upserted
