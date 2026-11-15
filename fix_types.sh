#!/usr/bin/env bash
# Converts sales_explorer_source.revenue from TEXT to REAL in Turso.
# If you see "$'\r': command not found", run once:  sed -i 's/\r$//' fix_types.sh
# Run with:  bash fix_types.sh
set -euo pipefail

DB=myturso
TABLE=sales_explorer_source

turso db shell "$DB" <<SQL
BEGIN;
CREATE TABLE ${TABLE}_new (
  date    TEXT NOT NULL,
  region  TEXT NOT NULL,
  revenue REAL NOT NULL
);
INSERT INTO ${TABLE}_new (date, region, revenue)
  SELECT date, region, CAST(revenue AS REAL) FROM ${TABLE};
DROP TABLE ${TABLE};
ALTER TABLE ${TABLE}_new RENAME TO ${TABLE};
COMMIT;
SQL

echo "Result:"
turso db shell "$DB" ".schema $TABLE"