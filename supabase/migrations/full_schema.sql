-- ============================================================
-- BBTerminal — complete schema (all migrations combined)
-- Run this on a fresh Supabase project to set up everything.
-- ============================================================

-- Sequences
CREATE SEQUENCE IF NOT EXISTS company_id_seq;
CREATE SEQUENCE IF NOT EXISTS portfolio_id_seq;
CREATE SEQUENCE IF NOT EXISTS backtest_run_id_seq;
CREATE SEQUENCE IF NOT EXISTS benchmark_id_seq;

-- ─── Company ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS company (
  company_id        INTEGER PRIMARY KEY DEFAULT nextval('company_id_seq'),
  longequity_ticker VARCHAR,
  primary_ticker    VARCHAR NOT NULL,
  primary_exchange  VARCHAR NOT NULL DEFAULT 'UNKNOWN',
  country           VARCHAR,
  company_name      VARCHAR,
  sector            VARCHAR,
  UNIQUE (primary_ticker, primary_exchange)
);

-- ─── Metric data (unified — all frequencies, all sources) ───
CREATE TABLE IF NOT EXISTS metric_data (
  company_id    INTEGER          NOT NULL REFERENCES company(company_id),
  metric_code   VARCHAR          NOT NULL,
  source_code   VARCHAR          NOT NULL,
  target_date   DATE             NOT NULL,
  numeric_value DOUBLE PRECISION,
  text_value    VARCHAR,
  is_prediction BOOLEAN          NOT NULL DEFAULT FALSE,
  recorded_at   TIMESTAMP        DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (company_id, metric_code, source_code, target_date)
);

CREATE INDEX IF NOT EXISTS idx_metric_data_source_date
  ON metric_data (source_code, target_date);

CREATE INDEX IF NOT EXISTS idx_metric_data_metric_company
  ON metric_data (metric_code, company_id, target_date);

-- ─── Portfolio ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS portfolio (
  portfolio_id   INTEGER PRIMARY KEY DEFAULT nextval('portfolio_id_seq'),
  portfolio_name VARCHAR NOT NULL,
  target_date    DATE    NOT NULL,
  published_at   DATE,
  UNIQUE (portfolio_name, target_date)
);

CREATE TABLE IF NOT EXISTS portfolio_weight (
  portfolio_id INTEGER          NOT NULL REFERENCES portfolio(portfolio_id),
  company_id   INTEGER          NOT NULL REFERENCES company(company_id),
  weight_value DOUBLE PRECISION NOT NULL CHECK (weight_value >= 0 AND weight_value <= 1),
  PRIMARY KEY (portfolio_id, company_id)
);

-- ─── Ticker override (OpenFIGI resolutions) ─────────────────
CREATE TABLE IF NOT EXISTS ticker_override (
  ticker           VARCHAR PRIMARY KEY,
  primary_ticker   VARCHAR   NOT NULL,
  primary_exchange VARCHAR   NOT NULL,
  source           VARCHAR   NOT NULL DEFAULT 'openfigi',
  created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ─── AIRS performance ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS airs_performance (
  portefeuille         TEXT NOT NULL,
  periode              DATE NOT NULL,
  beginvermogen        NUMERIC,
  koersresultaat       NUMERIC,
  opbrengsten          NUMERIC,
  beleggingsresultaat  NUMERIC,
  eindvermogen         NUMERIC,
  rendement            NUMERIC,
  cumulatief_rendement NUMERIC,
  fetched_at           TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (portefeuille, periode)
);

-- ─── Backtest runs ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS backtest_run (
  run_id          INTEGER PRIMARY KEY DEFAULT nextval('backtest_run_id_seq'),
  name            VARCHAR NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  config          JSONB NOT NULL,
  summary         JSONB NOT NULL,
  monthly_records JSONB NOT NULL,
  universe        JSONB NOT NULL
);

-- ─── Benchmarks ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS benchmark (
  benchmark_id    INTEGER PRIMARY KEY DEFAULT nextval('benchmark_id_seq'),
  ticker          VARCHAR NOT NULL UNIQUE,
  name            VARCHAR NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS benchmark_price (
  benchmark_id    INTEGER NOT NULL REFERENCES benchmark(benchmark_id) ON DELETE CASCADE,
  target_date     DATE NOT NULL,
  price           DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (benchmark_id, target_date)
);

-- ─── Universe snapshots ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS universe_snapshot (
  label         TEXT    NOT NULL DEFAULT 'default',
  target_month  TEXT    NOT NULL,  -- "YYYY-MM"
  company_id    INTEGER NOT NULL REFERENCES company(company_id) ON DELETE CASCADE,
  total_score   INTEGER NOT NULL DEFAULT 0,
  scores        JSONB   NOT NULL DEFAULT '{}',
  details       JSONB   NOT NULL DEFAULT '{}',
  passes        BOOLEAN NOT NULL DEFAULT FALSE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (label, target_month, company_id)
);

-- Migrate existing table: add label column if missing
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'universe_snapshot' AND column_name = 'label'
  ) THEN
    ALTER TABLE universe_snapshot ADD COLUMN label TEXT NOT NULL DEFAULT 'default';
    ALTER TABLE universe_snapshot DROP CONSTRAINT IF EXISTS universe_snapshot_pkey;
    ALTER TABLE universe_snapshot ADD PRIMARY KEY (label, target_month, company_id);
  END IF;
END $$;

DROP INDEX IF EXISTS idx_universe_snapshot_month;
CREATE INDEX IF NOT EXISTS idx_universe_snapshot_label_month
  ON universe_snapshot (label, target_month);

CREATE OR REPLACE FUNCTION universe_month_counts(p_label TEXT)
RETURNS TABLE(month TEXT, total BIGINT, passing BIGINT) AS $$
  SELECT
    target_month AS month,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE passes) AS passing
  FROM universe_snapshot
  WHERE label = p_label
  GROUP BY target_month
  ORDER BY target_month;
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION universe_labels()
RETURNS TABLE(label TEXT, start_month TEXT, end_month TEXT, month_count BIGINT, avg_passing BIGINT) AS $$
  SELECT
    label,
    MIN(target_month) AS start_month,
    MAX(target_month) AS end_month,
    COUNT(DISTINCT target_month) AS month_count,
    (COUNT(*) FILTER (WHERE passes) / NULLIF(COUNT(DISTINCT target_month), 0)) AS avg_passing
  FROM universe_snapshot
  GROUP BY label
  ORDER BY label;
$$ LANGUAGE sql STABLE;

-- ─── API usage tracking ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS api_usage (
  id SERIAL PRIMARY KEY,
  month TEXT NOT NULL,
  region TEXT NOT NULL,
  request_count INTEGER NOT NULL DEFAULT 0,
  UNIQUE(month, region)
);

-- ─── RPC functions ──────────────────────────────────────────
CREATE OR REPLACE FUNCTION get_distinct_dates(p_source_code text)
RETURNS TABLE(target_date date) AS $$
  SELECT DISTINCT md.target_date
  FROM metric_data md
  WHERE md.source_code = p_source_code
  ORDER BY md.target_date;
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION get_company_ids_for_date(p_source_code text, p_target_date date)
RETURNS TABLE(company_id int) AS $$
  SELECT DISTINCT md.company_id
  FROM metric_data md
  WHERE md.source_code = p_source_code
    AND md.target_date = p_target_date;
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION increment_api_usage(p_month TEXT, p_region TEXT, p_count INTEGER)
RETURNS VOID AS $$
BEGIN
  INSERT INTO api_usage (month, region, request_count)
  VALUES (p_month, p_region, p_count)
  ON CONFLICT (month, region)
  DO UPDATE SET request_count = api_usage.request_count + p_count;
END;
$$ LANGUAGE plpgsql;

-- Merge metric_data from one company into another, skipping conflicts
CREATE OR REPLACE FUNCTION merge_company_data(p_from_id INTEGER, p_to_id INTEGER)
RETURNS VOID AS $$
BEGIN
  -- Move metric_data rows that won't conflict
  UPDATE metric_data
  SET company_id = p_to_id
  WHERE company_id = p_from_id
    AND (metric_code, source_code, target_date) NOT IN (
      SELECT metric_code, source_code, target_date
      FROM metric_data WHERE company_id = p_to_id
    );
  -- Delete remaining rows from the source company
  DELETE FROM metric_data WHERE company_id = p_from_id;
END;
$$ LANGUAGE plpgsql;

-- ─── Reload PostgREST schema cache ─────────────────────────
NOTIFY pgrst, 'reload schema';
