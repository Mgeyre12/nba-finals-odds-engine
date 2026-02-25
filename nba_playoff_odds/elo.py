from __future__ import annotations


def expected_home_win_prob(home_elo: float, away_elo: float, home_court_adv: float = 65.0) -> float:
    adjusted_home = home_elo + home_court_adv
    exponent = (away_elo - adjusted_home) / 400.0
    return 1.0 / (1.0 + 10.0 ** exponent)


def update_elo(
    home_elo: float,
    away_elo: float,
    home_won: bool,
    k_factor: float = 20.0,
    home_court_adv: float = 65.0,
) -> tuple[float, float]:
    exp_home = expected_home_win_prob(home_elo=home_elo, away_elo=away_elo, home_court_adv=home_court_adv)
    actual_home = 1.0 if home_won else 0.0

    delta = k_factor * (actual_home - exp_home)
    return home_elo + delta, away_elo - delta
