from nba_playoff_odds.elo import expected_home_win_prob, update_elo


def test_expected_home_win_probability_has_home_edge() -> None:
    prob = expected_home_win_prob(home_elo=1500, away_elo=1500, home_court_adv=65)
    assert prob > 0.5


def test_elo_update_conserves_total_rating() -> None:
    new_home, new_away = update_elo(
        home_elo=1520,
        away_elo=1480,
        home_won=True,
        k_factor=20,
        home_court_adv=65,
    )
    assert round(new_home + new_away, 8) == 3000.0
