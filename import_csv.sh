#!/usr/bin/env bash
# If you see "$'\r': command not found", run once:  sed -i 's/\r$//' import_csv.sh
# Run with:  bash import_csv.sh
set -euo pipefail

CSV_FILE=sales_explorer_source.csv
TABLE=sales_explorer_source
EXISTING_DB=myturso

# stop if a table with this name already exists
FOUND=$(turso db shell "$EXISTING_DB" "SELECT name FROM sqlite_master WHERE name='$TABLE';")
if [[ "$FOUND" == *"$TABLE"* ]]; then
  echo "Table $TABLE already exists - nothing changed."; exit 1
fi

# build the table locally with the correct types, load the CSV (skipping the header row)
rm -f temp.db
sqlite3 temp.db <<SQL
CREATE TABLE $TABLE (
  date    TEXT NOT NULL,
  region  TEXT NOT NULL,
  revenue REAL NOT NULL
);
.import --csv --skip 1 $CSV_FILE $TABLE
SQL

# dump it and load the dump into Turso
sqlite3 temp.db .dump > dump.sql
turso db shell "$EXISTING_DB" < dump.sql
rm -f temp.db dump.sql

echo "Result:"
turso db shell "$EXISTING_DB" ".schema $TABLE"
turso db shell "$EXISTING_DB" "SELECT COUNT(*) FROM $TABLE;"