"""
Basic DAG integrity checks. Run with pytest.

These don't execute any tasks -- they just confirm the DAG file
imports cleanly, has no import errors, and has no cycles. This is
the kind of cheap, high-signal check that's worth running in CI
even though it can't catch runtime issues (bad data, failed API
calls, etc.) -- those require the real environment.
"""

import os
import sys

from airflow.models import DagBag

DAGS_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")


def test_no_import_errors():
    """Every DAG file in dags/ should import without raising an exception."""
    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    assert not dag_bag.import_errors, (
        f"DAG import errors found: {dag_bag.import_errors}"
    )


def test_nfl_pipeline_dag_loaded():
    """The nfl_data_pipeline DAG should be discoverable by Airflow.

    Uses dag_bag.dags (populated purely from parsing the file) rather
    than dag_bag.get_dag(), which additionally queries Airflow's
    metadata database -- a database that only exists once `airflow db
    migrate` has run (e.g. inside our Docker setup), not in a bare CI
    checkout. Reading from the parsed dict is the correct check here:
    we're validating the file, not the presence of a live database.
    """
    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    assert "nfl_data_pipeline" in dag_bag.dags, "nfl_data_pipeline DAG not found"


def test_nfl_pipeline_has_expected_tasks():
    """Sanity check that all four expected tasks are present."""
    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    dag = dag_bag.dags["nfl_data_pipeline"]
    task_ids = set(dag.task_ids)
    expected = {"extract_pbp", "extract_rosters", "dbt_run", "dbt_test"}
    assert expected.issubset(task_ids), (
        f"Missing expected tasks. Found: {task_ids}"
    )


def test_nfl_pipeline_has_no_cycles():
    """DagBag parsing already validates for cycles on load (a cyclic
    DAG would show up in dag_bag.import_errors), but this makes the
    intent explicit and fails loudly with a clear message if that
    ever changes."""
    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    dag = dag_bag.dags["nfl_data_pipeline"]
    dag.test_cycle()  # raises AirflowDagCycleException if a cycle exists