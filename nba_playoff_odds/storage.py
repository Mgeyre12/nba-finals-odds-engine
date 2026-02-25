from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from nba_playoff_odds.models import GameResult, TeamStanding


class DuckDBStorage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path))

    def _init_schema(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS bronze_standings_raw (
                    season INTEGER,
                    fetched_at TIMESTAMP,
                    payload JSON
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS bronze_games_raw (
                    season INTEGER,
                    fetched_at TIMESTAMP,
                    payload JSON
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_games (
                    game_id BIGINT PRIMARY KEY,
                    season INTEGER,
                    game_date TIMESTAMP,
                    home_team_id INTEGER,
                    home_team_name VARCHAR,
                    away_team_id INTEGER,
                    away_team_name VARCHAR,
                    home_score INTEGER,
                    away_score INTEGER,
                    postseason BOOLEAN
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_standings (
                    season INTEGER,
                    team_id INTEGER,
                    team_name VARCHAR,
                    conference VARCHAR,
                    wins INTEGER,
                    losses INTEGER,
                    win_pct DOUBLE,
                    PRIMARY KEY (season, team_id)
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS gold_championship_odds (
                    season INTEGER,
                    team_id INTEGER,
                    team_name VARCHAR,
                    championship_odds DOUBLE,
                    simulations INTEGER,
                    run_ts TIMESTAMP
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS gold_conference_odds (
                    season INTEGER,
                    conference VARCHAR,
                    team_id INTEGER,
                    team_name VARCHAR,
                    conference_odds DOUBLE,
                    simulations INTEGER,
                    run_ts TIMESTAMP
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS gold_finals_matchups (
                    season INTEGER,
                    matchup VARCHAR,
                    probability DOUBLE,
                    simulations INTEGER,
                    run_ts TIMESTAMP
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS gold_runs (
                    season INTEGER,
                    run_ts TIMESTAMP,
                    k_factor DOUBLE,
                    home_court_adv DOUBLE,
                    simulations INTEGER
                );
                """
            )

    def insert_bronze_payload(self, table: str, season: int, payload: list[dict[str, Any]]) -> None:
        if table not in {"bronze_standings_raw", "bronze_games_raw"}:
            raise ValueError(f"Unsupported bronze table: {table}")
        with self._connect() as con:
            con.execute(
                f"INSERT INTO {table}(season, fetched_at, payload) VALUES (?, ?, ?)",
                [season, datetime.utcnow(), json.dumps(payload)],
            )

    def upsert_silver_games(self, games: list[GameResult]) -> None:
        rows = [
            (
                g.game_id,
                g.season,
                g.game_date,
                g.home_team_id,
                g.home_team_name,
                g.away_team_id,
                g.away_team_name,
                g.home_score,
                g.away_score,
                g.postseason,
            )
            for g in games
        ]
        with self._connect() as con:
            con.executemany(
                """
                INSERT OR REPLACE INTO silver_games(
                    game_id, season, game_date, home_team_id, home_team_name,
                    away_team_id, away_team_name, home_score, away_score, postseason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def replace_silver_standings(self, standings: list[TeamStanding]) -> None:
        if not standings:
            return
        season = standings[0].season
        with self._connect() as con:
            con.execute("DELETE FROM silver_standings WHERE season = ?", [season])
            con.executemany(
                """
                INSERT INTO silver_standings(season, team_id, team_name, conference, wins, losses, win_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        s.season,
                        s.team_id,
                        s.team_name,
                        s.conference,
                        s.wins,
                        s.losses,
                        s.win_pct,
                    )
                    for s in standings
                ],
            )

    def load_silver_games(self, season: int) -> list[GameResult]:
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT game_id, season, game_date, home_team_id, home_team_name,
                       away_team_id, away_team_name, home_score, away_score, postseason
                FROM silver_games
                WHERE season = ?
                ORDER BY game_date, game_id
                """,
                [season],
            ).fetchall()
        return [
            GameResult(
                game_id=r[0],
                season=r[1],
                game_date=r[2],
                home_team_id=r[3],
                home_team_name=r[4],
                away_team_id=r[5],
                away_team_name=r[6],
                home_score=r[7],
                away_score=r[8],
                postseason=r[9],
            )
            for r in rows
        ]

    def load_silver_standings(self, season: int) -> list[TeamStanding]:
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT season, team_id, team_name, conference, wins, losses
                FROM silver_standings
                WHERE season = ?
                """,
                [season],
            ).fetchall()
        return [
            TeamStanding(
                season=r[0],
                team_id=r[1],
                team_name=r[2],
                conference=r[3],
                wins=r[4],
                losses=r[5],
            )
            for r in rows
        ]

    def write_gold_outputs(
        self,
        season: int,
        run_ts: datetime,
        k_factor: float,
        home_court_adv: float,
        simulations: int,
        championship_odds: dict[int, tuple[str, float]],
        conference_odds: dict[str, dict[int, tuple[str, float]]],
        finals_matchups: list[tuple[str, float]],
    ) -> None:
        with self._connect() as con:
            con.execute("DELETE FROM gold_championship_odds WHERE season = ?", [season])
            con.execute("DELETE FROM gold_conference_odds WHERE season = ?", [season])
            con.execute("DELETE FROM gold_finals_matchups WHERE season = ?", [season])
            con.execute("DELETE FROM gold_runs WHERE season = ?", [season])

            con.executemany(
                """
                INSERT INTO gold_championship_odds(season, team_id, team_name, championship_odds, simulations, run_ts)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (season, team_id, team_name, prob, simulations, run_ts)
                    for team_id, (team_name, prob) in championship_odds.items()
                ],
            )

            conference_rows: list[tuple[Any, ...]] = []
            for conference, team_probs in conference_odds.items():
                for team_id, (team_name, prob) in team_probs.items():
                    conference_rows.append((season, conference, team_id, team_name, prob, simulations, run_ts))
            con.executemany(
                """
                INSERT INTO gold_conference_odds(
                    season, conference, team_id, team_name, conference_odds, simulations, run_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                conference_rows,
            )

            con.executemany(
                """
                INSERT INTO gold_finals_matchups(season, matchup, probability, simulations, run_ts)
                VALUES (?, ?, ?, ?, ?)
                """,
                [(season, matchup, prob, simulations, run_ts) for matchup, prob in finals_matchups],
            )

            con.execute(
                """
                INSERT INTO gold_runs(season, run_ts, k_factor, home_court_adv, simulations)
                VALUES (?, ?, ?, ?, ?)
                """,
                [season, run_ts, k_factor, home_court_adv, simulations],
            )
