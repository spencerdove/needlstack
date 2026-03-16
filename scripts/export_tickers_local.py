"""
Export tickers.json locally for the search index.

This lightweight script exports ONLY tickers.json (all tickers from DB)
to docs/data/tickers.json. Per-ticker data is served from R2 in prod.

Usage:
    python scripts/export_tickers_local.py
"""

import json
import sys
from pathlib import Path

import sqlalchemy as sa

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.schema import get_engine

DOCS_DATA_DIR = Path(__file__).parent.parent / "docs" / "data"


def main() -> None:
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT ticker, company_name, sector, industry, asset_type, exchange "
                "FROM tickers ORDER BY ticker"
            )
        ).fetchall()

    tickers = [
        {
            "ticker": r[0],
            "company_name": r[1],
            "sector": r[2],
            "industry": r[3],
            "asset_type": r[4],
            "exchange": r[5],
        }
        for r in rows
    ]

    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DOCS_DATA_DIR / "tickers.json"
    out_path.write_text(json.dumps(tickers, separators=(",", ":")))
    print(f"Exported {len(tickers)} tickers to {out_path}")


if __name__ == "__main__":
    main()
