"""
Example Airflow DAG structure: NFL data pipeline writing to DuckDB.

Layout mirrors a raw -> staging -> marts warehouse pattern.
DuckDB lives as a single file on a mounted volume, written to
sequentially by each task (DuckDB does not support concurrent
writers, so tasks that touch the DB must run in series, not
parallel -- that constraint is itself worth calling out in your
README as a design decision).
"""

from datetime import datetime, timedelta

import duckdb
import nfl_data_py as nfl
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

DB_PATH = "/opt/airflow/data/nfl_warehouse.duckdb"

default_args = {
    "owner": "yeb",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="nfl_data_pipeline",
    default_args=default_args,
    description="Weekly NFL data ingestion -> staging -> marts",
    schedule_interval="0 6 * * 2",  # Tuesdays 6am, after MNF
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["nfl", "warehouse"],
)


def extract_pbp(**context):
    """Pull play-by-play data via nfl_data_py, land as raw table."""
    season = context["logical_date"].year
    df = nfl.import_pbp_data([season])

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE OR REPLACE TABLE raw.pbp AS SELECT * FROM df")
    con.close()


def extract_rosters(**context):
    """Pull weekly rosters, land as raw table."""
    season = context["logical_date"].year
    df = nfl.import_weekly_rosters([season])

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE OR REPLACE TABLE raw.rosters AS SELECT * FROM df")
    con.close()


def build_staging(**context):
    """
    Clean/rename raw tables into staging.
    In a real build, this is where you'd run `dbt run --select staging`
    instead of raw SQL -- shown inline here just to illustrate the layer.
    """
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("""
        CREATE OR REPLACE TABLE staging.pbp_clean AS
        SELECT
            game_id,
            week,
            posteam AS possession_team,
            defteam AS defense_team,
            play_type,
            yards_gained,
            epa
        FROM raw.pbp
        WHERE play_type IS NOT NULL
    """)
    con.close()


def build_marts(**context):
    """Aggregate staging into analysis-ready fact tables."""
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS marts")
    con.execute("""
        CREATE OR REPLACE TABLE marts.team_week_epa AS
        SELECT
            possession_team AS team,
            week,
            COUNT(*) AS plays,
            AVG(epa) AS avg_epa
        FROM staging.pbp_clean
        GROUP BY possession_team, week
    """)
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

staging_task = PythonOperator(
    task_id="build_staging",
    python_callable=build_staging,
    dag=dag,
)

marts_task = PythonOperator(
    task_id="build_marts",
    python_callable=build_marts,
    dag=dag,
)

# Extraction tasks can run in parallel (separate raw tables),
# but staging/marts must run after both, and in series with
# each other since they share the DuckDB file.
[extract_pbp_task, extract_rosters_task] >> staging_task >> marts_task
