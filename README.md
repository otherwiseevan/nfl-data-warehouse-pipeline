# NFL Data Warehouse Pipeline

A batch data pipeline that pulls NFL play-by-play and roster data, lands it in a DuckDB warehouse, and transforms it into analysis-ready tables using dbt. Orchestrated with Airflow, running in Docker.

This started as an extension of an existing fantasy football ML project — the goal was to build proper data infrastructure (ingestion, orchestration, transformation, testing) instead of one-off scripts.

## Architecture

```
nfl_data_py  -->  raw tables (DuckDB)  -->  dbt staging  -->  dbt marts
                                                                  |
                                                          fantasy football ML project
```

Airflow runs two Python tasks that pull data from `nfl_data_py` and write it into `raw` tables in a DuckDB file. Once both extraction tasks finish, Airflow shells out to dbt, which builds staging views (cleaned, renamed columns) and mart tables (team-week and player-week aggregates) on top of the raw data, then runs a set of tests against the results.

## Stack

- **Airflow** (LocalExecutor) — orchestration
- **DuckDB** — warehouse, stored as a single file
- **dbt** — transformation and testing
- **Docker Compose** — everything runs in containers except the DuckDB file itself, which lives on a mounted volume so it persists between runs

DuckDB doesn't support concurrent writers, so dbt is configured to run single-threaded (`threads: 1`), and the two extraction tasks each open and close their own connection rather than sharing one.

## Running it

Requires Docker Desktop.

```bash
docker compose build
docker compose up airflow-init
docker compose up
```

Airflow UI is at `localhost:8080` (login: admin/admin — fine for local use, not meant for anything beyond that). The DAG is `nfl_data_pipeline`. Trigger it manually from the UI or with:

```bash
docker compose exec airflow-scheduler airflow dags trigger nfl_data_pipeline
```

To run dbt directly against the warehouse without going through Airflow (useful while iterating on models):

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && export DBT_DUCKDB_PATH=/opt/airflow/data/nfl_warehouse.duckdb && dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt --threads 1"
```

## Data quality

dbt tests run automatically as the last step of the DAG. A few of them are set to warn instead of fail, because the underlying nulls are expected, not defects:

- `sleeper_id` is null for players `nfl_data_py` doesn't have a Sleeper cross-reference for.
- `player_id` is null for a small number of Development Squad / International Player Pathway players, who don't get the standard roster ID.

Play-by-play rows with `play_type = no_play` (timeouts, presnap penalties, etc.) are filtered out in staging rather than tested for, since they don't represent an offensive play and don't belong in a team-performance aggregate.

## Project structure

```
dags/           Airflow DAG
dbt/            dbt project (staging + marts models, tests, macros)
tests/          DAG integrity tests (pytest)
data/           DuckDB warehouse file (gitignored, generated at runtime)
```

## CI

GitHub Actions runs on every push and PR:
- DAG integrity checks (imports cleanly, has the expected tasks, no cycles)
- `dbt parse` to catch broken model references or invalid YAML

Neither of these touches real data — CI has no populated warehouse to test against, so `dbt test` itself only runs as part of the actual pipeline, not in CI.

## Notes on schema naming

dbt's default behavior concatenates a model's custom schema with the connection's default schema, so `staging` becomes `main_staging`. That's standard dbt behavior on any adapter, not specific to DuckDB. `dbt/macros/generate_schema_name.sql` overrides it so models land in `staging` and `marts` directly.
