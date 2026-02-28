"""
One-time database initialization script.

Usage:
    python scripts/init_db.py
"""
import sys
from pathlib import Path

# Allow running from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from db.schema import DB_PATH, init_db

if __name__ == "__main__":
    engine = init_db()
    print(f"Database initialized at: {DB_PATH.resolve()}")
