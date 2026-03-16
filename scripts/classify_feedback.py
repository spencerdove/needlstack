#!/usr/bin/env python3
"""
Classify all unclassified feedback using Claude.

Usage:
    python scripts/classify_feedback.py
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from db.schema import init_db
from ingestion.feedback_classifier import classify_feedback

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    engine = init_db()
    count = classify_feedback(engine=engine)
    print(f"Classified {count} feedback item(s)")


if __name__ == "__main__":
    main()
