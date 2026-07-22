# NFL Data Warehouse Pipeline

A batch data pipeline that pulls NFL play-by-play and roster data, along with a live snapshot of my Sleeper dynasty league, lands it in a DuckDB warehouse, and transforms it into analysis-ready tables using dbt. Orchestrated with Airflow, running in Docker.

This started as an extension of an existing fantasy football ML project — the goal was to build proper data infrastructure (ingestion, orchestration, transformation, testing) instead of one-off scripts, and to have a real warehouse to build a Streamlit app on top of.

## Architecture

```
nfl_data_py (2024-present)  --\
                                 --> raw tables (DuckDB) --> dbt staging --> dbt marts
Sleeper API (live snapshot) --/                                                |
                                                                       Streamlit app (planned)
```

Airflow runs three extraction tasks in parallel: two pull historical play-by-play and roster data from `nfl_data_py` (2024 season through the current one), and a third pulls a live snapshot of my actual Sleeper league (rosters, owners, current record) from Sleeper's public API. All three land as `raw` tables in a DuckDB file. Once extraction finishes, Airflow shells out to dbt, which builds staging views and mart tables on top of the raw data, then runs tests against the results.

The Sleeper data joins to the nfl_data_py data on `sleeper_id`, a column nfl_data_py already provides as part of its roster data's cross-platform ID mapping — no fuzzy matching needed.

## Stack

- **Airflow** (LocalExecutor) — orchestration
- **DuckDB** — warehouse, stored as a single file
- **dbt** — transformation and testing
- **Docker Compose** — everything runs in containers except the DuckDB file itself, which lives on a mounted volume so it persists between runs

DuckDB doesn't support concurrent writers, so dbt is configured to run single-threaded (`threads: 1`), and each extraction task opens and closes its own connection rather than sharing one.

## Running it

Requires Docker Desktop.

```bash
docker compose build
docker compose up airflow-init
docker compose up
```

Airflow UI is at `localhost:8080` (login: admin/admin — fine for local use, not meant for anything beyond that; this whole stack only runs on localhost and isn't exposed to anyone else). The DAG is `nfl_data_pipeline`. Trigger it manually from the UI or with:

```bash
docker compose exec airflow-scheduler airflow dags trigger nfl_data_pipeline
```

To run dbt directly against the warehouse without going through Airflow (useful while iterating on models):

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && export DBT_DUCKDB_PATH=/opt/airflow/data/nfl_warehouse.duckdb && dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt --threads 1"
```

### Sleeper league ID

The Sleeper extraction task reads a league ID from an Airflow Variable rather than a hardcoded value or a config file, so it never ends up in this repo. Set your own before running the DAG:

```bash
docker compose exec airflow-scheduler airflow variables set SLEEPER_LEAGUE_ID <your_league_id>
```

Find your league ID in the URL when viewing your league on sleeper.com.

## Data quality

dbt tests run automatically as the last step of the DAG. A few of them are set to warn instead of fail, because the underlying nulls are expected, not defects:

- `sleeper_id` is null for players `nfl_data_py` doesn't have a Sleeper cross-reference for.
- `player_id` is null for a small number of Development Squad / International Player Pathway players, who don't get the standard roster ID.

Play-by-play rows with `play_type = no_play` (timeouts, presnap penalties, etc.) are filtered out in staging rather than tested for, since they don't represent an offensive play and don't belong in a team-performance aggregate.

`nfl_data_py` has a couple of internal bugs around multi-season pulls — passing a list of seasons directly can throw errors that have nothing to do with the actual data (a duplicate-index issue in roster age calculation, and a broken exception handler that masks a simple 404 for a season that hasn't happened yet). Both extraction tasks pull one season at a time and concatenate the results instead, which avoids both issues.

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

## Next up

- A staging/mart model joining `raw.sleeper_rosters` against `fct_player_week` on `sleeper_id`, giving one clean table of "my current roster with real performance stats."
- A Streamlit app on top of that mart: tracking my own roster's per-game stats (targets, yards, TDs), and surfacing players outside my roster who are trending well, as a trade/waiver research tool.