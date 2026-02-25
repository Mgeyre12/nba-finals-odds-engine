from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class TeamStanding:
    season: int
    team_id: int
    team_name: str
    conference: str
    wins: int
    losses: int

    @property
    def win_pct(self) -> float:
        total = self.wins + self.losses
        return (self.wins / total) if total else 0.0


@dataclass(frozen=True)
class GameResult:
    game_id: int
    season: int
    game_date: datetime
    home_team_id: int
    home_team_name: str
    away_team_id: int
    away_team_name: str
    home_score: int
    away_score: int
    postseason: bool


@dataclass(frozen=True)
class SeededTeam:
    team_id: int
    team_name: str
    conference: str
    seed: int
