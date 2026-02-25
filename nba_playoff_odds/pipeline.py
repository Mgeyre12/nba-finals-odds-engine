from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from nba_playoff_odds.api import BallDontLieClient
from nba_playoff_odds.bracket import build_playoff_field
from nba_playoff_odds.config import Settings
from nba_playoff_odds.elo import update_elo
from nba_playoff_odds.models import GameResult, TeamStanding
from nba_playoff_odds.simulation import run_monte_carlo
from nba_playoff_odds.storage import DuckDBStorage

logger = logging.getLogger(__name__)


def _team_name(team_payload: dict[str, Any]) -> str:
    full_name = team_payload.get("full_name")
    if full_name:
        return str(full_name)
    city = str(team_payload.get("city", "")).strip()
    name = str(team_payload.get("name", "")).strip()
    return f"{city} {name}".strip() or f"Team {team_payload.get('id', 'unknown')}"


def parse_standings(raw_rows: list[dict[str, Any]], season: int) -> list[TeamStanding]:
    out: list[TeamStanding] = []
    for row in raw_rows:
        team = row.get("team", {})
        team_id = team.get("id") or row.get("team_id")
        if team_id is None:
            continue

        conference = row.get("conference") or team.get("conference")
        conference = str(conference or "").strip().title()
        if conference not in {"East", "West"}:
            continue

        wins = row.get("wins")
        losses = row.get("losses")
        if wins is None or losses is None:
            record = str(row.get("record", ""))
            if "-" in record:
                left, right = record.split("-", 1)
                wins = int(left)
                losses = int(right)
            else:
                continue

        out.append(
            TeamStanding(
                season=season,
                team_id=int(team_id),
                team_name=_team_name(team),
                conference=conference,
                wins=int(wins),
                losses=int(losses),
            )
        )
    return out


def parse_games(raw_rows: list[dict[str, Any]], season: int) -> list[GameResult]:
    out: list[GameResult] = []
    for row in raw_rows:
        if row.get("postseason", False):
            continue
        status = str(row.get("status", "")).lower()
        if "final" not in status:
            continue

        home_team = row.get("home_team") or {}
        away_team = row.get("visitor_team") or row.get("away_team") or {}
        game_date = row.get("date") or row.get("datetime")
        if not game_date:
            continue

        out.append(
            GameResult(
                game_id=int(row["id"]),
                season=season,
                game_date=datetime.fromisoformat(str(game_date).replace("Z", "+00:00")),
                home_team_id=int(home_team["id"]),
                home_team_name=_team_name(home_team),
                away_team_id=int(away_team["id"]),
                away_team_name=_team_name(away_team),
                home_score=int(row.get("home_team_score", 0)),
                away_score=int(row.get("visitor_team_score", row.get("away_team_score", 0))),
                postseason=bool(row.get("postseason", False)),
            )
        )
    return out


def build_standings_from_games(raw_games: list[dict[str, Any]], season: int) -> list[TeamStanding]:
    team_rows: dict[int, dict[str, Any]] = {}
    for row in raw_games:
        if row.get("postseason", False):
            continue
        status = str(row.get("status", "")).lower()
        if "final" not in status:
            continue

        home = row.get("home_team") or {}
        away = row.get("visitor_team") or row.get("away_team") or {}
        if not home or not away:
            continue

        home_id = int(home["id"])
        away_id = int(away["id"])
        home_score = int(row.get("home_team_score", 0))
        away_score = int(row.get("visitor_team_score", row.get("away_team_score", 0)))

        for team in (home, away):
            team_id = int(team["id"])
            if team_id not in team_rows:
                team_rows[team_id] = {
                    "team_id": team_id,
                    "team_name": _team_name(team),
                    "conference": str(team.get("conference", "")).strip().title(),
                    "wins": 0,
                    "losses": 0,
                }

        if home_score > away_score:
            team_rows[home_id]["wins"] += 1
            team_rows[away_id]["losses"] += 1
        elif away_score > home_score:
            team_rows[away_id]["wins"] += 1
            team_rows[home_id]["losses"] += 1

    standings: list[TeamStanding] = []
    for row in team_rows.values():
        conference = row["conference"]
        if conference not in {"East", "West"}:
            continue
        standings.append(
            TeamStanding(
                season=season,
                team_id=row["team_id"],
                team_name=row["team_name"],
                conference=conference,
                wins=row["wins"],
                losses=row["losses"],
            )
        )
    return standings


def build_regular_season_ratings(
    games: list[GameResult],
    k_factor: float,
    home_court_adv: float,
) -> dict[int, float]:
    ratings: dict[int, float] = {}
    for game in sorted(games, key=lambda g: (g.game_date, g.game_id)):
        home_elo = ratings.get(game.home_team_id, 1500.0)
        away_elo = ratings.get(game.away_team_id, 1500.0)
        home_won = game.home_score > game.away_score
        ratings[game.home_team_id], ratings[game.away_team_id] = update_elo(
            home_elo=home_elo,
            away_elo=away_elo,
            home_won=home_won,
            k_factor=k_factor,
            home_court_adv=home_court_adv,
        )
    return ratings


def _write_json(path: Path, payload: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def backfill_season(client: BallDontLieClient, storage: DuckDBStorage, settings: Settings, season: int) -> None:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    games_raw = client.get_regular_season_games(season)
    standings_raw: list[dict[str, Any]] = []
    standings: list[TeamStanding]

    try:
        standings_raw = client.get_standings(season)
        standings = parse_standings(standings_raw, season)
    except RuntimeError as exc:
        if "Unauthorized" in str(exc):
            logger.warning(
                "Standings endpoint unauthorized for this API key/tier; deriving standings from games instead"
            )
            standings = build_standings_from_games(games_raw, season)
        else:
            raise

    storage.insert_bronze_payload("bronze_standings_raw", season=season, payload=standings_raw)
    storage.insert_bronze_payload("bronze_games_raw", season=season, payload=games_raw)

    _write_json(settings.bronze_dir / f"standings_{season}_{ts}.json", standings_raw)
    _write_json(settings.bronze_dir / f"games_{season}_{ts}.json", games_raw)

    games = parse_games(games_raw, season)
    storage.replace_silver_standings(standings)
    storage.upsert_silver_games(games)

    logger.info("Backfill complete for season=%s (standings=%s, games=%s)", season, len(standings), len(games))


def run_daily_pipeline(
    client: BallDontLieClient,
    storage: DuckDBStorage,
    settings: Settings,
    season: int,
    n_simulations: int,
    k_factor: float,
    home_court_adv: float,
    seed: int = 7,
) -> None:
    backfill_season(client=client, storage=storage, settings=settings, season=season)

    games = storage.load_silver_games(season)
    standings = storage.load_silver_standings(season)

    ratings = build_regular_season_ratings(games=games, k_factor=k_factor, home_court_adv=home_court_adv)
    field = build_playoff_field(standings)

    championship_odds, conference_odds, finals_matchups = run_monte_carlo(
        playoff_field=field,
        base_ratings=ratings,
        n_simulations=n_simulations,
        k_factor=k_factor,
        home_court_adv=home_court_adv,
        seed=seed,
    )

    run_ts = datetime.utcnow()
    storage.write_gold_outputs(
        season=season,
        run_ts=run_ts,
        k_factor=k_factor,
        home_court_adv=home_court_adv,
        simulations=n_simulations,
        championship_odds=championship_odds,
        conference_odds=conference_odds,
        finals_matchups=finals_matchups,
    )

    champ_df = pd.DataFrame(
        [
            {"team_id": team_id, "team_name": team_name, "championship_odds": prob}
            for team_id, (team_name, prob) in championship_odds.items()
        ]
    ).sort_values("championship_odds", ascending=False)
    conf_rows: list[dict[str, Any]] = []
    for conference, items in conference_odds.items():
        for team_id, (team_name, prob) in items.items():
            conf_rows.append(
                {
                    "conference": conference,
                    "team_id": team_id,
                    "team_name": team_name,
                    "conference_odds": prob,
                }
            )
    conf_df = pd.DataFrame(conf_rows).sort_values(["conference", "conference_odds"], ascending=[True, False])
    finals_df = pd.DataFrame(
        [{"matchup": matchup, "probability": prob} for matchup, prob in finals_matchups]
    )

    champ_df.to_csv(settings.gold_dir / "championship_odds.csv", index=False)
    conf_df.to_csv(settings.gold_dir / "conference_odds.csv", index=False)
    finals_df.to_csv(settings.gold_dir / "finals_matchups_top10.csv", index=False)
    with (settings.gold_dir / "last_updated.txt").open("w", encoding="utf-8") as fh:
        fh.write(run_ts.isoformat())

    logger.info("Daily pipeline complete for season=%s", season)
