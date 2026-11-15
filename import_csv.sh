#!/usr/bin/env bash
#   sed -i 's/\r$//' import_csv.sh
#   Run with:  bash import_csv.sh
#   turso auth login --headless
#   curl -sSfL https://get.tur.so/install.sh | bash

set -euo pipefail

CSV_FILE=sales_explorer_source.csv
TABLE=sales_explorer_source
EXISTING_DB=myturso

# stop if a table with this name already exists
FOUND=$(turso db shell "$EXISTING_DB" "SELECT name FROM sqlite_master WHERE name='$TABLE';")
if [[ "$FOUND" == *"$TABLE"* ]]; then
  echo "Table $TABLE already exists - nothing changed."; exit 1
fi

# build the table locally from the CSV, dump it, load the dump into Turso
rm -f temp.db
sqlite3 temp.db ".import --csv $CSV_FILE $TABLE"
sqlite3 temp.db .dump > dump.sql
turso db shell "$EXISTING_DB" < dump.sql
rm -f temp.db

turso db shell "$EXISTING_DB" "SELECT COUNT(*) FROM $TABLE;"