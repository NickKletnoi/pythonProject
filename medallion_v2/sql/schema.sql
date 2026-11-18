-- Source schema for Turso (SQLite / libSQL dialect). Idempotent.
CREATE TABLE IF NOT EXISTS customers (
  customer_id INTEGER PRIMARY KEY,
  name        TEXT,
  email       TEXT,
  segment     TEXT,
  country     TEXT,
  updated_at  TEXT NOT NULL            -- 'YYYY-MM-DD HH:MM:SS', drives incremental extraction
);
CREATE TABLE IF NOT EXISTS products (
  product_id   INTEGER PRIMARY KEY,
  product_name TEXT,
  category     TEXT,
  list_price   REAL,
  updated_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sales (
  order_line_id INTEGER PRIMARY KEY,
  order_id      INTEGER,
  customer_id   INTEGER,
  product_id    INTEGER,
  quantity      INTEGER,
  unit_price    REAL,
  order_ts      TEXT,
  updated_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_customers_updated ON customers(updated_at);
CREATE INDEX IF NOT EXISTS ix_products_updated  ON products(updated_at);
CREATE INDEX IF NOT EXISTS ix_sales_updated     ON sales(updated_at);
