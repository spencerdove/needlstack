"""
Store feedback entries in the database.
"""
import logging
from datetime import datetime
from typing import Optional

import sqlalchemy as sa

from db.schema import get_engine, feedback_table

logger = logging.getLogger(__name__)


def store_feedback(
    body: str,
    source: str = "email",
    sender_email: Optional[str] = None,
    sender_name: Optional[str] = None,
    subject: Optional[str] = None,
    changelog_version: Optional[str] = None,
    engine: Optional[sa.Engine] = None,
) -> int:
    """Insert a feedback row and return its id."""
    if engine is None:
        engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(
            feedback_table.insert().values(
                source=source,
                sender_email=sender_email,
                sender_name=sender_name,
                subject=subject,
                body=body,
                received_at=datetime.utcnow(),
                changelog_version=changelog_version,
            )
        )
        conn.commit()
        row_id = result.lastrowid
        logger.info(f"Stored feedback id={row_id} from={sender_email} version={changelog_version}")
        return row_id
