from __future__ import annotations

import random
from collections import defaultdict

from nba_playoff_odds.elo import expected_home_win_prob, update_elo
from nba_playoff_odds.models import SeededTeam, TeamStanding


def build_playoff_field(standings: list[TeamStanding]) -> dict[str, list[SeededTeam]]:
    conference_groups: dict[str, list[TeamStanding]] = defaultdict(list)
    for row in standings:
        conference = row.conference.strip().title()
        if conference in {"East", "West"}:
            conference_groups[conference].append(row)

    field: dict[str, list[SeededTeam]] = {}
    for conference in ("East", "West"):
        rows = sorted(
            conference_groups.get(conference, []),
            key=lambda s: (-s.win_pct, -s.wins, s.losses, s.team_name),
        )[:8]
        field[conference] = [
            SeededTeam(team_id=t.team_id, team_name=t.team_name, conference=conference, seed=i + 1)
            for i, t in enumerate(rows)
        ]

    if len(field.get("East", [])) < 8 or len(field.get("West", [])) < 8:
        raise ValueError("Could not build playoff field: both conferences need at least 8 teams in standings")

    return field


def _simulate_series(
    high_seed: SeededTeam,
    low_seed: SeededTeam,
    ratings: dict[int, float],
    rng: random.Random,
    k_factor: float,
    home_court_adv: float,
) -> SeededTeam:
    high_wins = 0
    low_wins = 0
    schedule_high_home = [True, True, False, False, True, False, True]

    for high_is_home in schedule_high_home:
        if high_wins == 4 or low_wins == 4:
            break

        if high_is_home:
            home = high_seed
            away = low_seed
        else:
            home = low_seed
            away = high_seed

        home_elo = ratings.get(home.team_id, 1500.0)
        away_elo = ratings.get(away.team_id, 1500.0)
        home_prob = expected_home_win_prob(home_elo, away_elo, home_court_adv)
        home_won = rng.random() < home_prob

        ratings[home.team_id], ratings[away.team_id] = update_elo(
            home_elo=home_elo,
            away_elo=away_elo,
            home_won=home_won,
            k_factor=k_factor,
            home_court_adv=home_court_adv,
        )

        high_team_won = (home.team_id == high_seed.team_id and home_won) or (
            away.team_id == high_seed.team_id and not home_won
        )
        if high_team_won:
            high_wins += 1
        else:
            low_wins += 1

    return high_seed if high_wins > low_wins else low_seed


def _series_with_home_court(
    team_a: SeededTeam,
    team_b: SeededTeam,
    ratings: dict[int, float],
    rng: random.Random,
    k_factor: float,
    home_court_adv: float,
) -> SeededTeam:
    if team_a.seed < team_b.seed:
        return _simulate_series(team_a, team_b, ratings, rng, k_factor, home_court_adv)
    if team_b.seed < team_a.seed:
        return _simulate_series(team_b, team_a, ratings, rng, k_factor, home_court_adv)
    return _simulate_series(
        team_a if ratings.get(team_a.team_id, 1500.0) >= ratings.get(team_b.team_id, 1500.0) else team_b,
        team_b if ratings.get(team_a.team_id, 1500.0) >= ratings.get(team_b.team_id, 1500.0) else team_a,
        ratings,
        rng,
        k_factor,
        home_court_adv,
    )


def simulate_playoffs(
    playoff_field: dict[str, list[SeededTeam]],
    ratings: dict[int, float],
    rng: random.Random,
    k_factor: float,
    home_court_adv: float,
) -> tuple[SeededTeam, SeededTeam, SeededTeam]:
    conf_champs: dict[str, SeededTeam] = {}

    for conference in ("East", "West"):
        teams = playoff_field[conference]
        by_seed = {t.seed: t for t in teams}

        qf1 = _simulate_series(by_seed[1], by_seed[8], ratings, rng, k_factor, home_court_adv)
        qf2 = _simulate_series(by_seed[4], by_seed[5], ratings, rng, k_factor, home_court_adv)
        qf3 = _simulate_series(by_seed[2], by_seed[7], ratings, rng, k_factor, home_court_adv)
        qf4 = _simulate_series(by_seed[3], by_seed[6], ratings, rng, k_factor, home_court_adv)

        sf1 = _series_with_home_court(qf1, qf2, ratings, rng, k_factor, home_court_adv)
        sf2 = _series_with_home_court(qf3, qf4, ratings, rng, k_factor, home_court_adv)
        conf_champs[conference] = _series_with_home_court(sf1, sf2, ratings, rng, k_factor, home_court_adv)

    east = conf_champs["East"]
    west = conf_champs["West"]

    east_rating = ratings.get(east.team_id, 1500.0)
    west_rating = ratings.get(west.team_id, 1500.0)
    if east_rating >= west_rating:
        champion = _simulate_series(east, west, ratings, rng, k_factor, home_court_adv)
    else:
        champion = _simulate_series(west, east, ratings, rng, k_factor, home_court_adv)
    return champion, east, west
