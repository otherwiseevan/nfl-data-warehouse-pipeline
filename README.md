# NFL Data Warehouse Pipeline

A batch data pipeline that pulls NFL play-by-play and roster data, along with a live snapshot of my Sleeper dynasty league, lands it in a DuckDB warehouse, and transforms it into analysis-ready tables using dbt. Orchestrated with Airflow, running in Docker. A Streamlit app on top reads directly from the warehouse for roster tracking and trade/waiver research.

This started as an extension of an existing fantasy football ML project — the goal was to build proper data infrastructure (ingestion, orchestration, transformation, testing) instead of one-off scripts, with a real app on top rather than a notebook.

## Architecture

```
nfl_data_py (2024-present)  --\
--> raw tables (DuckDB) --> dbt staging --> dbt marts --> Streamlit app
Sleeper API (live snapshot) --/
```

Airflow runs three extraction tasks in parallel: two pull historical play-by-play and roster data from `nfl_data_py` (2024 season through the current one), and a third pulls a live snapshot of my actual Sleeper league (rosters, owners, current record) from Sleeper's public API. All three land as `raw` tables in a DuckDB file. Once extraction finishes, Airflow shells out to dbt, which builds staging views and mart tables on top of the raw data, then runs tests against the results.

The Sleeper data joins to the nfl_data_py data on `sleeper_id`, a column nfl_data_py already provides as part of its roster data's cross-platform ID mapping — no fuzzy matching needed.

## Stack

- **Airflow** (LocalExecutor) — orchestration
- **DuckDB** — warehouse, stored as a single file
- **dbt** — transformation and testing
- **Docker Compose** — everything runs in containers except the DuckDB file itself, which lives on a mounted volume so it persists between runs
- **Streamlit** — reads the warehouse directly (read-only) for the roster/trade app, run separately from Docker

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

### Sleeper league and user ID

The Sleeper extraction task reads a league ID and my own Sleeper user ID from Airflow Variables, not hardcoded values or a config file, so neither ends up in this repo:

```bash
docker compose exec airflow-scheduler airflow variables set SLEEPER_LEAGUE_ID <your_league_id>
docker compose exec airflow-scheduler airflow variables set SLEEPER_USER_ID <your_user_id>
```

Find your league ID in the URL when viewing your league on sleeper.com. Find your user ID with `curl https://api.sleeper.app/v1/user/<your_username>`.

The pipeline identifies my own roster by matching `owner_id` against my real Sleeper `user_id`, not by display name — display names can be changed, and matching on the wrong one silently attributes someone else's roster to me. Worth knowing if you fork this: get this ID right before trusting the "my roster" view.

## Streamlit app

```bash
streamlit run streamlit_app/app.py
```

Two views: my current roster's stats (season totals and a per-player weekly trend chart), and a trade/waiver leaderboard split between players on other rosters in the league and players unrostered entirely. Both read from `fct_player_game_stats` and `fct_player_roster_status`, joined on `player_id`.

Kickers are filtered out of both views since my league doesn't roster that position.

The app runs outside Docker, directly against the DuckDB file on disk, in read-only mode. If Airflow is actively writing to the warehouse at the exact same moment, a query can briefly fail — DuckDB's single-writer constraint, not a bug in the app.

## Fantasy scoring

`fct_player_game_stats` aggregates play-by-play up to one row per player per game and computes `fantasy_points` using my league's actual scoring settings (half-PPR, Superflex, standard TD/yardage/kicking values — hardcoded in the model, not pulled dynamically from Sleeper's API yet).

A few things worth knowing about how this was built:

- There's no `targets` or `receptions` column in the raw data — both are derived (a target is any pass attempt where `receiver_player_id` is populated; a reception is a completed one).
- Fumble recovery touchdowns are credited using `return_touchdown`, not `touchdown`. `touchdown` fires for any score on the play, including one where the original ball carrier fought into the end zone and a teammate incidentally recovered a loose ball on the same play — crediting that to the recoverer would be wrong. Caught this by pulling real play descriptions before trusting the logic, not by assumption.
- Two-point conversions are attributed correctly across all three roles (passer, rusher, receiver) — verified against real play data.

## Data quality

dbt tests run automatically as the last step of the DAG. A few of them are set to warn instead of fail, because the underlying nulls are expected, not defects:

- `sleeper_id` is null for players `nfl_data_py` doesn't have a Sleeper cross-reference for.
- `player_id` is null for a small number of Development Squad / International Player Pathway players, who don't get the standard roster ID.

Play-by-play rows with `play_type = no_play` (timeouts, presnap penalties, etc.) are filtered out in staging rather than tested for, since they don't represent an offensive play and don't belong in a team-performance aggregate.

`nfl_data_py` has a couple of internal bugs around multi-season pulls — passing a list of seasons directly can throw errors that have nothing to do with the actual data (a duplicate-index issue in roster age calculation, and a broken exception handler that masks a simple 404 for a season that hasn't happened yet). Both extraction tasks pull one season at a time and concatenate the results instead, which avoids both issues.

pandas' `groupby` silently drops any row where a grouping key is `NaN` by default. The Streamlit app fills `owner_name` nulls with `"Free Agent"` *before* grouping, not after — filling afterward is too late, since by then `groupby` has already dropped every row in the unrostered player pool.

## Project structure

```
dags/            Airflow DAG
dbt/             dbt project (staging + marts models, tests, macros)
tests/           DAG integrity tests (pytest)
streamlit_app/   roster tracker + trade/waiver scouting app
data/            DuckDB warehouse file (gitignored, generated at runtime)
```

## CI

GitHub Actions runs on every push and PR:
- DAG integrity checks (imports cleanly, has the expected tasks, no cycles)
- `dbt parse` to catch broken model references or invalid YAML

Neither of these touches real data — CI has no populated warehouse to test against, so `dbt test` itself only runs as part of the actual pipeline, not in CI.

## Notes on schema naming

dbt's default behavior concatenates a model's custom schema with the connection's default schema, so `staging` becomes `main_staging`. That's standard dbt behavior on any adapter, not specific to DuckDB. `dbt/macros/generate_schema_name.sql` overrides it so models land in `staging` and `marts` directly.

## Next up

- Pull real scoring settings from Sleeper's `GET /league/<id>` endpoint instead of hardcoding the point values in the dbt model, so the mart stays correct automatically if league scoring ever changes.
- Two-point conversion and fumble recovery TD logic were spot-checked against a sample of real plays, not exhaustively validated across every season — worth a wider audit before trusting the totals for anything beyond directional use.
- Historical ownership tracking. `fct_player_roster_status` reflects current roster ownership only; it doesn't track who owned a player earlier in the season.
