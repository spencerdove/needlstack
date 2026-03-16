#!/usr/bin/env python3
"""
Send a changelog email to all subscribers.

Usage:
    python scripts/send_changelog.py v4.0 [--dry-run]
"""
import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from ingestion.email import send_changelog, parse_changelog

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_subscribers() -> list:
    path = Path(__file__).resolve().parent.parent / "data" / "subscribers.json"
    if not path.exists():
        logger.error(f"Subscribers file not found: {path}")
        sys.exit(1)
    with open(path) as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="Send changelog email to subscribers")
    parser.add_argument("version", help="Version to send (e.g. v4.0)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    args = parser.parse_args()

    subscribers = load_subscribers()
    logger.info(f"Loaded {len(subscribers)} subscriber(s)")

    if args.dry_run:
        changelog = parse_changelog(args.version)
        print(f"\n{'='*60}")
        print(f"Subject: Needlstack {changelog['version']} — {changelog['title']}")
        print(f"Date: {changelog['date']}")
        print(f"Recipients: {len(subscribers)}")
        print(f"{'='*60}")
        print(changelog["html"])
        print(f"{'='*60}\n")

    results = send_changelog(args.version, subscribers, dry_run=args.dry_run)

    sent = sum(1 for r in results if r["status"] == "sent")
    errors = sum(1 for r in results if r["status"] == "error")
    dry = sum(1 for r in results if r["status"] == "dry_run")

    if args.dry_run:
        logger.info(f"Dry run complete: {dry} email(s) previewed")
    else:
        logger.info(f"Done: {sent} sent, {errors} errors")

    if errors:
        for r in results:
            if r["status"] == "error":
                logger.error(f"  Failed: {r['to']} — {r['error']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
