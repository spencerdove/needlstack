"""
CLI entry point for the Needlstack AI agent.

Usage:
    python agent/cli.py "What is NVDA's revenue growth trend?"
    python agent/cli.py --tickers NVDA AMD "Compare these two chip makers"
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.schema import init_db
from agent.runner import run_agent


def main():
    parser = argparse.ArgumentParser(description="Needlstack AI Investment Research Agent")
    parser.add_argument("question", help="Research question to ask the agent")
    parser.add_argument("--tickers", nargs="+", help="Context tickers for the question")
    args = parser.parse_args()

    engine = init_db()
    response = run_agent(args.question, context_tickers=args.tickers, engine=engine)
    print(response)


if __name__ == "__main__":
    main()
