# Medallion pipeline: Turso → DuckDB (Bronze / Silver / Gold) in a GitHub Codespace

Source = a **Turso** database. Layers = **DuckDB** file `lake/warehouse.duckdb` (+ Parquet landing files). Same five steps as the Databricks version: stage, clean, join, merge to Gold, incremental delta.

## Run it
1. Push this folder to a GitHub repo and open it in a Codespace (the `.devcontainer` installs everything).
2. *(Optional, to use real Turso)* add Codespaces secrets `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN`, then rebuild the container. With no secrets the notebooks use a local SQLite stand-in, so everything still works.
3. Open `notebooks/01_load_turso_source.ipynb`, keep `BATCH = 1`, run all. (Creates the tables from `sql/schema.sql` and loads `data/*_batch1.csv`.)
4. Open `notebooks/02_medallion_pipeline.ipynb`, run all.
5. Set `BATCH = 2` in notebook 01, run it, then run notebook 02 again. Repeat with `BATCH = 3`.

Open notebooks from VS Code or JupyterLab so the kernel's working directory is the `notebooks/` folder.

## Data (import into Turso)
| File | Rows | Purpose |
|---|---|---|
| `sql/schema.sql` | | source tables + `updated_at` indexes |
| `data/{customers,products,sales}_batch1.csv` | 60 / 20 / 304 | initial load, includes deliberate bad data |
| `data/{customers,products,sales}_batch2.csv` | 14 / 5 / 105 | updates, new rows, sales for customers that don't exist yet |
| `data/customers_batch3.csv` | 2 | the missing customers arrive |

Import options: the notebook loader (notebook 01), or the Turso CLI (see the last cell of notebook 01). Blank CSV cells are NULLs; if imported as empty strings the Silver step treats them as NULL.

## Environment variables
`TURSO_DATABASE_URL`, `TURSO_AUTH_TOKEN` (source) · `TURSO_REMOTE=1` (query Turso directly instead of an embedded replica) · `LOAD_MODE=full` (rebuild Gold from all of Silver) · `BATCH`, `RESET_SOURCE=1` (notebook 01).

`tools/make_data.py` regenerates the mock CSVs (seeded, reproducible).
