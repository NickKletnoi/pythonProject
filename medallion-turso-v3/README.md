# Medallion in Turso (Codespaces)

Everything lives in one Turso database: source tables, `bronze_*`, `silver_*`, `gold_*` and `ctl_*`. No local database, no Parquet.

Folders: `notebooks/` (00_config, 01, 02, 03, 04), `domains/` (sales.yml, customer360.yml), `rules/` (dq_rules.yml), `sql/` (source schemas), `data/` (mock CSV batches).

1. Codespaces secrets: `TURSO_DATABASE_URL` (libsql://...) and `TURSO_AUTH_TOKEN`. Use the Codespace kernel.
2. Run 01 (BATCH=1), then 02. Repeat for batch 2 and 3.
3. Optional multi-source scenario: 03 then 04 (batch 1, then batch 2).
4. Start over for a domain: set RESET_SOURCE=1 for that domain's load notebook (01 or 03).
