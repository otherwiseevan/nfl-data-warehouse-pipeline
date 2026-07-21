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
    schedule_interval="0 6 * * 2",  # Tuesdays 6am, after MNF
    start_date=datetime(2024, 9, 1),
    catchup=False,
    tags=["nfl", "warehouse"],
)


def extract_pbp(**context):
    """Pull play-by-play data via nfl_data_py, land as raw table."""
    season = 2024  # hardcoded for now — 2026 season hasn't started yet

    df = nfl.import_pbp_data([season])
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE OR REPLACE TABLE raw.pbp AS SELECT * FROM df")
    con.close()


def extract_rosters(**context):
    """Pull weekly rosters, land as raw table."""
    season = 2024  # hardcoded for now — 2026 season hasn't started yet

    df = nfl.import_weekly_rosters([season])
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE OR REPLACE TABLE raw.rosters AS SELECT * FROM df")
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
[extract_pbp_task, extract_rosters_task] >> dbt_run_task >> dbt_test_task