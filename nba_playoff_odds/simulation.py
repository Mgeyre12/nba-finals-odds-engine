from __future__ import annotations

import random
from collections import Counter, defaultdict

from nba_playoff_odds.bracket import simulate_playoffs
from nba_playoff_odds.models import SeededTeam


def run_monte_carlo(
    playoff_field: dict[str, list[SeededTeam]],
    base_ratings: dict[int, float],
    n_simulations: int,
    k_factor: float,
    home_court_adv: float,
    seed: int = 7,
) -> tuple[
    dict[int, tuple[str, float]],
    dict[str, dict[int, tuple[str, float]]],
    list[tuple[str, float]],
]:
    if n_simulations <= 0:
        raise ValueError("n_simulations must be greater than 0")

    team_lookup = {
        t.team_id: t.team_name
        for conference in ("East", "West")
        for t in playoff_field[conference]
    }

    champ_counts: Counter[int] = Counter()
    conference_counts: dict[str, Counter[int]] = {"East": Counter(), "West": Counter()}
    finals_counts: Counter[str] = Counter()

    parent_rng = random.Random(seed)
    for _ in range(n_simulations):
        sim_rng = random.Random(parent_rng.randint(0, 10**9))
        sim_ratings = dict(base_ratings)
        champion, east_champ, west_champ = simulate_playoffs(
            playoff_field=playoff_field,
            ratings=sim_ratings,
            rng=sim_rng,
            k_factor=k_factor,
            home_court_adv=home_court_adv,
        )

        champ_counts[champion.team_id] += 1
        conference_counts["East"][east_champ.team_id] += 1
        conference_counts["West"][west_champ.team_id] += 1
        finals_counts[f"{east_champ.team_name} vs {west_champ.team_name}"] += 1

    championship_odds = {
        team_id: (team_lookup[team_id], count / n_simulations)
        for team_id, count in champ_counts.items()
    }

    conference_odds: dict[str, dict[int, tuple[str, float]]] = defaultdict(dict)
    for conference, counter in conference_counts.items():
        conference_odds[conference] = {
            team_id: (team_lookup[team_id], count / n_simulations) for team_id, count in counter.items()
        }

    finals_top_10 = [(matchup, count / n_simulations) for matchup, count in finals_counts.most_common(10)]
    return championship_odds, conference_odds, finals_top_10
