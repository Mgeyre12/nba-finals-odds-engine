import random

from nba_playoff_odds.models import SeededTeam
from nba_playoff_odds.simulation import run_monte_carlo


def _playoff_field() -> dict[str, list[SeededTeam]]:
    return {
        "East": [
            SeededTeam(team_id=100 + seed, team_name=f"East {seed}", conference="East", seed=seed)
            for seed in range(1, 9)
        ],
        "West": [
            SeededTeam(team_id=200 + seed, team_name=f"West {seed}", conference="West", seed=seed)
            for seed in range(1, 9)
        ],
    }


def _ratings() -> dict[int, float]:
    rng = random.Random(42)
    ratings: dict[int, float] = {}
    for team_id in list(range(101, 109)) + list(range(201, 209)):
        ratings[team_id] = 1500 + rng.uniform(-75, 75)
    return ratings


def test_monte_carlo_is_deterministic_with_seed() -> None:
    field = _playoff_field()
    ratings = _ratings()

    out_1 = run_monte_carlo(
        playoff_field=field,
        base_ratings=ratings,
        n_simulations=300,
        k_factor=20,
        home_court_adv=65,
        seed=123,
    )
    out_2 = run_monte_carlo(
        playoff_field=field,
        base_ratings=ratings,
        n_simulations=300,
        k_factor=20,
        home_court_adv=65,
        seed=123,
    )

    assert out_1 == out_2
