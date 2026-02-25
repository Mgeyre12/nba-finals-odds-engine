from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    project_root: Path
    data_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    db_path: Path
    api_base_url: str
    default_k_factor: float
    default_home_court_adv: float
    default_simulations: int
    min_request_interval_seconds: float

    @staticmethod
    def from_env() -> "Settings":
        project_root = Path(__file__).resolve().parent.parent
        data_dir = project_root / "data"
        return Settings(
            project_root=project_root,
            data_dir=data_dir,
            bronze_dir=data_dir / "bronze",
            silver_dir=data_dir / "silver",
            gold_dir=data_dir / "gold",
            db_path=data_dir / "nba.duckdb",
            api_base_url=os.getenv("BALLDONTLIE_BASE_URL", "https://api.balldontlie.io/v1"),
            default_k_factor=float(os.getenv("ELO_K_FACTOR", "20")),
            default_home_court_adv=float(os.getenv("ELO_HOME_COURT_ADV", "65")),
            default_simulations=int(os.getenv("MONTE_CARLO_SIMS", "10000")),
            min_request_interval_seconds=float(os.getenv("BDL_MIN_REQUEST_INTERVAL_SECONDS", "12.5")),
        )


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def load_settings() -> Settings:
    settings = Settings.from_env()
    load_dotenv(dotenv_path=settings.project_root / ".env", override=False)
    load_dotenv(override=False)
    settings = Settings.from_env()
    settings.bronze_dir.mkdir(parents=True, exist_ok=True)
    settings.silver_dir.mkdir(parents=True, exist_ok=True)
    settings.gold_dir.mkdir(parents=True, exist_ok=True)
    return settings


def infer_season(today: datetime | None = None) -> int:
    now = today or datetime.utcnow()
    return now.year if now.month < 10 else now.year + 1
