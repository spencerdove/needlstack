#!/usr/bin/env python3
"""
Print a summary report of classified feedback.

Usage:
    python scripts/feedback_report.py [--version v4.0] [--category bug]
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

import sqlalchemy as sa

from db.schema import get_engine, init_db, feedback_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    parser = argparse.ArgumentParser(description="Feedback report")
    parser.add_argument("--version", help="Filter by changelog version")
    parser.add_argument("--category", help="Filter by category (bug, feature_request, question, general)")
    args = parser.parse_args()

    engine = init_db()

    with engine.connect() as conn:
        query = sa.select(feedback_table).order_by(feedback_table.c.received_at.desc())
        if args.version:
            query = query.where(feedback_table.c.changelog_version == args.version)
        if args.category:
            query = query.where(feedback_table.c.category == args.category)

        rows = conn.execute(query).fetchall()

    if not rows:
        print("No feedback found.")
        return

    # Group by category
    by_category = {}
    for row in rows:
        cat = row.category or "unclassified"
        by_category.setdefault(cat, []).append(row)

    print(f"\n{'='*60}")
    print(f"Feedback Report — {len(rows)} item(s)")
    if args.version:
        print(f"Version: {args.version}")
    print(f"{'='*60}\n")

    for cat, items in sorted(by_category.items()):
        print(f"## {cat.upper()} ({len(items)})")
        print("-" * 40)
        for item in items:
            sender = item.sender_email or "unknown"
            summary = item.summary or item.body[:80]
            confidence = f" ({item.category_confidence:.0%})" if item.category_confidence else ""
            actioned = " [ACTIONED]" if item.is_actioned else ""
            print(f"  [{item.id}] {sender}: {summary}{confidence}{actioned}")
        print()


if __name__ == "__main__":
    main()
