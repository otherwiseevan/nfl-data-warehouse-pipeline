"""
NFL data pipeline: raw -> staging -> marts, in DuckDB.

Extraction (pulling from nfl_data_py) stays as PythonOperator tasks,
since that's genuine Python logic. Staging and marts are now handled
by dbt instead of raw SQL strings -- dbt owns the transformation
layer, which gives us lineage, docs, and tests (dbt test) for free.
"""

from datetime import datetime, timedelta

import duckdb
import nfl_data_py as nfl
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import pandas as pd

from airflow.models import Variable

DB_PATH = "/opt/airflow/data/nfl_warehouse.duckdb"
DBT_PROJECT_DIR = "/opt/airflow/dbt"

# dbt commands need to run from the project dir, with profiles.yml
# resolved from the same folder (repo-local profiles, not ~/.dbt/),
# and DBT_DUCKDB_PATH pointing at the container's mounted DB path.
DBT_ENV_PREFIX = (
    f"cd {DBT_PROJECT_DIR} && "
    f"export DBT_DUCKDB_PATH={DB_PATH} && "
)

default_args = {
    "owner": "yeb",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="nfl_data_pipeline",
    default_args=default_args,
    description="Weekly NFL data ingestion -> dbt staging -> dbt marts",
    schedule="0 6 * * 2",  # Tuesdays 6am, after MNF
    start_date=datetime(2024, 9, 1),
    catchup=False,
    tags=["nfl", "warehouse"],
)


def extract_pbp(**context):
    """Pull play-by-play data via nfl_data_py, land as raw table.
    Pulled one season at a time and concatenated (see extract_rosters
    for why). Also wrapped per-season in a try/except -- nfl_data_py
    throws a confusing secondary NameError instead of a clean message
    when a season's data doesn't exist yet (e.g. the current season
    before it's started), so this catches that and just skips the
    season rather than failing the whole task."""

    current_year = datetime.now().year
    seasons = list(range(2024, current_year + 1))

    frames = []
    for season in seasons:
        try:
            frames.append(nfl.import_pbp_data([season]))
        except Exception as e:
            print(f"Skipping season {season}: {e}")

    df = pd.concat(frames, ignore_index=True)

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE OR REPLACE TABLE raw.pbp AS SELECT * FROM df")
    con.close()


def extract_rosters(**context):
    """Pull weekly rosters, land as raw table. Same per-season +
    try/except pattern as extract_pbp."""

    current_year = datetime.now().year
    seasons = list(range(2024, current_year + 1))

    frames = []
    for season in seasons:
        try:
            frames.append(nfl.import_weekly_rosters([season]))
        except Exception as e:
            print(f"Skipping season {season}: {e}")

    df = pd.concat(frames, ignore_index=True)

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE OR REPLACE TABLE raw.rosters AS SELECT * FROM df")
    con.close()

import requests




def extract_sleeper_league(**context):
    """Pull current league rosters and team/owner info from the
    Sleeper API. Unlike nfl_data_py's historical data, this is a
    live snapshot of your specific league right now -- there's no
    concept of 'season' here in the same sense, just current state.
    Sleeper's player_id matches the sleeper_id column already
    present in stg_rosters/fct_player_week, so this joins directly
    without any fuzzy matching."""
    league_id = Variable.get("SLEEPER_LEAGUE_ID")
    rosters_resp = requests.get(
        f"https://api.sleeper.app/v1/league/{league_id}/rosters"
    )
    rosters_resp.raise_for_status()
    rosters = rosters_resp.json()

    users_resp = requests.get(
        f"https://api.sleeper.app/v1/league/{league_id}/users"
    )
    users_resp.raise_for_status()
    users = users_resp.json()

    # users keyed by user_id so we can look up owner display names
    users_by_id = {u["user_id"]: u.get("display_name", "unknown") for u in users}

    # Flatten: one row per player per roster, tagging the owner name
    rows = []
    for roster in rosters:
        owner_name = users_by_id.get(roster.get("owner_id"), "unknown")
        for player_id in roster.get("players") or []:
            rows.append({
                "roster_id": roster["roster_id"],
                "owner_id": roster.get("owner_id"),
                "owner_name": owner_name,
                "sleeper_id": player_id,
                "wins": roster.get("settings", {}).get("wins"),
                "losses": roster.get("settings", {}).get("losses"),
            })

    df = pd.DataFrame(rows)

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE OR REPLACE TABLE raw.sleeper_rosters AS SELECT * FROM df")
    con.close()

extract_pbp_task = PythonOperator(
    task_id="extract_pbp",
    python_callable=extract_pbp,
    dag=dag,
)

extract_rosters_task = PythonOperator(
    task_id="extract_rosters",
    python_callable=extract_rosters,
    dag=dag,
)

extract_sleeper_task = PythonOperator(
    task_id="extract_sleeper_league",
    python_callable=extract_sleeper_league,
    dag=dag,
)

# --threads 1 matches profiles.yml -- DuckDB can't handle concurrent
# writers, so dbt's model parallelism has to be turned off here.
dbt_run_task = BashOperator(
    task_id="dbt_run",
    bash_command=(
        DBT_ENV_PREFIX
        + f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --threads 1"
    ),
    dag=dag,
)

dbt_test_task = BashOperator(
    task_id="dbt_test",
    bash_command=(
        DBT_ENV_PREFIX
        + f"dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --threads 1"
    ),
    dag=dag,
)

# Extraction tasks can run in parallel (separate raw tables, separate
# connections that open/close quickly). dbt run must wait on both,
# and dbt test must wait on dbt run.
[extract_pbp_task, extract_rosters_task, extract_sleeper_task] >> dbt_run_task >> dbt_test_task