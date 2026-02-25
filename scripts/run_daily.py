from __future__ import annotations

import argparse
import sys

from nba_playoff_odds.api import BallDontLieClient
from nba_playoff_odds.config import configure_logging, infer_season, load_settings
from nba_playoff_odds.pipeline import run_daily_pipeline
from nba_playoff_odds.storage import DuckDBStorage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run daily NBA playoff odds pipeline")
    parser.add_argument("--season", type=int, default=None, help="Season year (default inferred)")
    parser.add_argument("--sims", type=int, default=None, help="Number of Monte Carlo simulations")
    parser.add_argument("--k-factor", type=float, default=None, help="Elo K-factor")
    parser.add_argument("--home-adv", type=float, default=None, help="Home-court Elo adjustment")
    parser.add_argument("--seed", type=int, default=7, help="Monte Carlo RNG seed")
    return parser.parse_args()


def main() -> int:
    configure_logging()
    settings = load_settings()
    args = parse_args()

    season = args.season if args.season is not None else infer_season()
    sims = args.sims if args.sims is not None else settings.default_simulations
    k_factor = args.k_factor if args.k_factor is not None else settings.default_k_factor
    home_adv = args.home_adv if args.home_adv is not None else settings.default_home_court_adv

    try:
        client = BallDontLieClient(
            base_url=settings.api_base_url,
            min_request_interval_seconds=settings.min_request_interval_seconds,
        )
        storage = DuckDBStorage(db_path=settings.db_path)
        run_daily_pipeline(
            client=client,
            storage=storage,
            settings=settings,
            season=season,
            n_simulations=sims,
            k_factor=k_factor,
            home_court_adv=home_adv,
            seed=args.seed,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
