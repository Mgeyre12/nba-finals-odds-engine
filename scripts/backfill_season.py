from __future__ import annotations

import argparse
import sys

from nba_playoff_odds.api import BallDontLieClient
from nba_playoff_odds.config import configure_logging, load_settings
from nba_playoff_odds.pipeline import backfill_season
from nba_playoff_odds.storage import DuckDBStorage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill NBA regular season games + standings")
    parser.add_argument("season", type=int, help="Season year")
    return parser.parse_args()


def main() -> int:
    configure_logging()
    settings = load_settings()
    args = parse_args()

    try:
        client = BallDontLieClient(
            base_url=settings.api_base_url,
            min_request_interval_seconds=settings.min_request_interval_seconds,
        )
        storage = DuckDBStorage(db_path=settings.db_path)
        backfill_season(client=client, storage=storage, settings=settings, season=args.season)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
