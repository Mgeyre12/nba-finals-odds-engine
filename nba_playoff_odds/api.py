from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)


class BallDontLieClient:
    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        timeout: int = 30,
        min_request_interval_seconds: float = 0.0,
        max_retries: int = 5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.min_request_interval_seconds = max(0.0, min_request_interval_seconds)
        self.max_retries = max(1, max_retries)
        self._last_request_ts = 0.0
        self.api_key = (api_key or os.getenv("BALLDONTLIE_API_KEY") or "").strip()
        if not self.api_key:
            raise RuntimeError(
                "BALLDONTLIE_API_KEY is not set. Add it to your environment or .env file. "
                "See .env.example for the expected format."
            )

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": self.api_key,
                "x-api-key": self.api_key,
                "Accept": "application/json",
            }
        )

    def _request(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        for attempt in range(1, self.max_retries + 1):
            elapsed = time.time() - self._last_request_ts
            if elapsed < self.min_request_interval_seconds:
                time.sleep(self.min_request_interval_seconds - elapsed)

            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                self._last_request_ts = time.time()
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status == 429 and attempt < self.max_retries:
                    retry_after_raw = exc.response.headers.get("Retry-After", "") if exc.response is not None else ""
                    retry_after = float(retry_after_raw) if retry_after_raw.isdigit() else 15.0
                    sleep_seconds = max(retry_after, 5.0 * attempt)
                    logger.warning(
                        "Rate limited by BALLDONTLIE (429). Retrying in %.1fs (attempt %s/%s)",
                        sleep_seconds,
                        attempt,
                        self.max_retries,
                    )
                    time.sleep(sleep_seconds)
                    continue

                msg = exc.response.text[:300] if exc.response is not None else str(exc)
                raise RuntimeError(f"BALLDONTLIE request failed ({url}): {msg}") from exc
            except requests.RequestException as exc:
                if attempt < self.max_retries:
                    sleep_seconds = 2.0 * attempt
                    logger.warning(
                        "Network error calling BALLDONTLIE; retrying in %.1fs (attempt %s/%s)",
                        sleep_seconds,
                        attempt,
                        self.max_retries,
                    )
                    time.sleep(sleep_seconds)
                    continue
                raise RuntimeError(f"Network error calling BALLDONTLIE ({url}): {exc}") from exc

        raise RuntimeError(f"BALLDONTLIE request failed after {self.max_retries} attempts ({url})")

    def _get_paginated(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        cursor: int | None = None
        out: list[dict[str, Any]] = []
        while True:
            req_params = dict(params)
            if cursor is not None:
                req_params["cursor"] = cursor
            payload = self._request(endpoint=endpoint, params=req_params)
            rows = payload.get("data", [])
            out.extend(rows)

            meta = payload.get("meta", {})
            next_cursor = meta.get("next_cursor")
            if not next_cursor or not rows:
                break
            cursor = int(next_cursor)
        return out

    def get_standings(self, season: int) -> list[dict[str, Any]]:
        logger.info("Fetching standings for season=%s", season)
        params = {"season": season, "per_page": 100}
        return self._get_paginated("standings", params)

    def get_regular_season_games(self, season: int) -> list[dict[str, Any]]:
        logger.info("Fetching regular season games for season=%s", season)
        params = {"seasons[]": season, "per_page": 100}
        games = self._get_paginated("games", params)
        return [g for g in games if not g.get("postseason", False)]
