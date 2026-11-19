-- Five source tables for the multi-source customer scenario (Turso / SQLite dialect). Idempotent.
CREATE TABLE IF NOT EXISTS crm_accounts (
  account_id INTEGER PRIMARY KEY, full_name TEXT, email TEXT, phone TEXT, segment TEXT, country TEXT, owner TEXT,
  updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS billing_customers (
  billing_id INTEGER PRIMARY KEY, email TEXT, billing_name TEXT, plan TEXT, status TEXT, mrr REAL, country TEXT,
  updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS support_contacts (
  contact_id INTEGER PRIMARY KEY, email TEXT, name TEXT, phone TEXT, tier TEXT, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS web_visitors (
  visitor_id INTEGER PRIMARY KEY, email TEXT, first_seen TEXT, last_seen TEXT, visits INTEGER, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS marketing_leads (
  lead_id INTEGER PRIMARY KEY, email TEXT, name TEXT, lead_source TEXT, lead_status TEXT, marketing_consent TEXT,
  updated_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ix_crm_updated       ON crm_accounts(updated_at);
CREATE INDEX IF NOT EXISTS ix_billing_updated   ON billing_customers(updated_at);
CREATE INDEX IF NOT EXISTS ix_support_updated   ON support_contacts(updated_at);
CREATE INDEX IF NOT EXISTS ix_web_updated       ON web_visitors(updated_at);
CREATE INDEX IF NOT EXISTS ix_marketing_updated ON marketing_leads(updated_at);
