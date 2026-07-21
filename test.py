import duckdb
con = duckdb.connect("data/nfl_warehouse.duckdb")

# see what tables exist across all schemas
con.sql("SELECT table_schema, table_name FROM information_schema.tables").show()

# peek at a mart
con.sql("SELECT * FROM marts.fct_team_week_epa LIMIT 10").show()