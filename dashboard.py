from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "data" / "nba.duckdb"


@st.cache_data(ttl=300)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        runs = con.execute("SELECT * FROM gold_runs ORDER BY run_ts DESC").fetch_df()
        if runs.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        season = int(runs.iloc[0]["season"])
        run_ts = runs.iloc[0]["run_ts"]

        champs = con.execute(
            """
            SELECT team_name, championship_odds
            FROM gold_championship_odds
            WHERE season = ? AND run_ts = ?
            ORDER BY championship_odds DESC
            """,
            [season, run_ts],
        ).fetch_df()
        conf = con.execute(
            """
            SELECT conference, team_name, conference_odds
            FROM gold_conference_odds
            WHERE season = ? AND run_ts = ?
            ORDER BY conference, conference_odds DESC
            """,
            [season, run_ts],
        ).fetch_df()
        finals = con.execute(
            """
            SELECT matchup, probability
            FROM gold_finals_matchups
            WHERE season = ? AND run_ts = ?
            ORDER BY probability DESC
            LIMIT 10
            """,
            [season, run_ts],
        ).fetch_df()
    return champs, conf, finals, runs.head(1)


def main() -> None:
    st.set_page_config(page_title="NBA Playoff Odds", layout="wide")
    st.title("NBA Championship Odds Dashboard")

    if not DB_PATH.exists():
        st.warning("DuckDB file not found. Run scripts/run_daily.py first.")
        return

    champs, conf, finals, runs = load_data()
    if runs.empty:
        st.warning("No gold outputs found. Run scripts/run_daily.py first.")
        return

    st.caption(f"Last updated: {runs.iloc[0]['run_ts']}")
    st.subheader("Title Odds")
    st.bar_chart(champs.set_index("team_name")["championship_odds"])

    st.subheader("Conference Win Odds")
    left, right = st.columns(2)
    east = conf[conf["conference"] == "East"].copy()
    west = conf[conf["conference"] == "West"].copy()

    east["conference_odds"] = (east["conference_odds"] * 100).round(2)
    west["conference_odds"] = (west["conference_odds"] * 100).round(2)
    left.dataframe(east.rename(columns={"conference_odds": "Conference Odds (%)"}), hide_index=True)
    right.dataframe(west.rename(columns={"conference_odds": "Conference Odds (%)"}), hide_index=True)

    st.subheader("Most Likely Finals Matchups")
    finals_out = finals.copy()
    finals_out["probability"] = (finals_out["probability"] * 100).round(2)
    st.dataframe(finals_out.rename(columns={"probability": "Probability (%)"}), hide_index=True)


if __name__ == "__main__":
    main()
