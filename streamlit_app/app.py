"""
Fantasy roster tracker, reading directly from the DuckDB warehouse
built by the Airflow/dbt pipeline. Two views: my current roster's
performance, and trade/waiver targets outside my roster.

Run from the project root:
    streamlit run streamlit_app/app.py
"""

import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "nfl_warehouse.duckdb")
DB_PATH = os.path.normpath(DB_PATH)

st.set_page_config(page_title="Dynasty Roster Tracker", layout="wide")

print("=== SCRIPT START ===", flush=True)


@st.cache_resource
def get_connection():
    print("get_connection: opening DuckDB connection", flush=True)
    con = duckdb.connect(DB_PATH, read_only=True)
    print("get_connection: connection opened OK", flush=True)
    return con


@st.cache_data(ttl=300)
def load_roster_status():
    print("load_roster_status: start", flush=True)
    con = get_connection()
    df = con.sql("SELECT * FROM marts.fct_player_roster_status").df()
    print(f"load_roster_status: loaded {df.shape}", flush=True)
    return df


@st.cache_data(ttl=300)
def load_game_stats():
    print("load_game_stats: start", flush=True)
    con = get_connection()
    df = con.sql("SELECT * FROM marts.fct_player_game_stats").df()
    print(f"load_game_stats: loaded {df.shape}", flush=True)
    return df


print("calling load_roster_status()", flush=True)
roster_status = load_roster_status()

print("calling load_game_stats()", flush=True)
game_stats = load_game_stats()

roster_status = roster_status.copy(deep=True)
game_stats = game_stats.copy(deep=True)

print("merging roster_status + game_stats", flush=True)
combined = game_stats.merge(
    roster_status[["player_id", "roster_id", "owner_name", "is_my_roster", "position"]].drop_duplicates("player_id"),
    on="player_id",
    how="left",
)
combined = combined[combined["position"] != "K"]

print(f"merge done: {combined.shape}", flush=True)

seasons = sorted(combined["season"].unique(), reverse=True)
print(f"seasons available: {seasons}", flush=True)

st.title("Dynasty Roster Tracker")

view = st.sidebar.radio("View", ["My Roster", "Trade & Waiver Targets"])
print(f"view selected: {view}", flush=True)

season = st.sidebar.selectbox("Season", seasons)
print(f"season selected: {season}", flush=True)

season_data = combined[combined["season"] == season]
print(f"season_data filtered: {season_data.shape}", flush=True)

if view == "My Roster":
    st.header("My Roster")
    print("entering My Roster branch", flush=True)

    my_data = season_data[season_data["is_my_roster"] == True]
    print(f"my_data filtered: {my_data.shape}", flush=True)

    if my_data.empty:
        st.info("No performance data yet for the selected season.")
        print("my_data is empty, showing info message", flush=True)
    else:
        print("computing season_totals groupby", flush=True)
        season_totals = (
            my_data.groupby(["player_name", "position"], as_index=False)
            .agg(
                games=("week", "nunique"),
                fantasy_points=("fantasy_points", "sum"),
                targets=("targets", "sum"),
                receptions=("receptions", "sum"),
                receiving_yards=("receiving_yards", "sum"),
                rushing_yards=("rushing_yards", "sum"),
                passing_yards=("passing_yards", "sum"),
                total_tds=("passing_tds", lambda s: s.sum())
            )
            .sort_values("fantasy_points", ascending=False)
        )
        print(f"season_totals computed: {season_totals.shape}", flush=True)

        st.dataframe(season_totals, use_container_width=True)
        print("st.dataframe rendered", flush=True)

        selected_player = st.selectbox("View weekly trend for:", season_totals["player_name"])
        print(f"selected_player: {selected_player}", flush=True)

        player_weeks = my_data[my_data["player_name"] == selected_player].sort_values("week")
        print(f"player_weeks filtered: {player_weeks.shape}", flush=True)

        print("building plotly figure", flush=True)
        fig = px.line(
            player_weeks,
            x="week",
            y="fantasy_points",
            markers=True,
            title=f"{selected_player} -- fantasy points by week ({season})",
        )
        print("plotly figure built OK", flush=True)

        st.plotly_chart(fig, use_container_width=True)
        print("st.plotly_chart rendered", flush=True)

        stat_cols = st.columns(3)
        with stat_cols[0]:
            st.metric("Targets", int(player_weeks["targets"].sum()))
        with stat_cols[1]:
            st.metric("Receiving Yards", int(player_weeks["receiving_yards"].sum()))
        with stat_cols[2]:
            st.metric("Rushing Yards", int(player_weeks["rushing_yards"].sum()))
        print("st.metric columns rendered", flush=True)

else:
    st.header("Trade & Waiver Targets")
    print("entering Trade & Waiver Targets branch", flush=True)

    pool = st.radio(
        "Player pool",
        ["Unrostered in my league", "On another roster (trade targets)"],
        horizontal=True,
    )
    print(f"pool selected: {pool}", flush=True)

    if pool == "Unrostered in my league":
        pool_data = season_data[season_data["owner_name"].isna()]
    else:
        pool_data = season_data[
            (season_data["owner_name"].notna()) & (season_data["is_my_roster"] != True)
        ]
    print(f"pool_data filtered: {pool_data.shape}", flush=True)

    # Fill NaN owner_name BEFORE grouping, not after -- groupby drops
    # any row where a grouping key is NaN by default, and every row
    # in the "Unrostered" pool has a NaN owner_name. Filling after
    # grouping is too late; the rows are already gone by then.
    pool_data = pool_data.copy()
    pool_data["owner_name"] = pool_data["owner_name"].fillna("Free Agent")

    min_week = st.slider("Minimum games played", 1, 17, 3)
    print(f"min_week selected: {min_week}", flush=True)

    print("computing leaderboard groupby", flush=True)
    leaderboard = (
        pool_data.groupby(["player_name", "position", "team", "owner_name"], as_index=False)
        .agg(
            games=("week", "nunique"),
            total_fantasy_points=("fantasy_points", "sum"),
            avg_fantasy_points=("fantasy_points", "mean"),
            targets=("targets", "sum"),
            rushing_yards=("rushing_yards", "sum"),
            receiving_yards=("receiving_yards", "sum"),
        )
    )
    print(f"leaderboard computed: {leaderboard.shape}", flush=True)

    leaderboard = leaderboard[leaderboard["games"] >= min_week]
    leaderboard["avg_fantasy_points"] = leaderboard["avg_fantasy_points"].round(2)
    leaderboard = leaderboard.sort_values("avg_fantasy_points", ascending=False)
    print(f"leaderboard filtered/sorted: {leaderboard.shape}", flush=True)

    st.dataframe(leaderboard, use_container_width=True)
    print("st.dataframe rendered", flush=True)

print("=== SCRIPT END ===", flush=True)