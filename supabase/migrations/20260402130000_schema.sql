-- ============================================================
-- BBTerminal — full schema (single file)
-- ============================================================

-- Sequences
CREATE SEQUENCE IF NOT EXISTS company_id_seq;
CREATE SEQUENCE IF NOT EXISTS portfolio_id_seq;

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

-- ─── RPC helpers ─────────────────────────────────────────────
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

-- ─── Ticker override (OpenFIGI resolutions) ─────────────────
CREATE TABLE IF NOT EXISTS ticker_override (
  ticker           VARCHAR PRIMARY KEY,
  primary_ticker   VARCHAR   NOT NULL,
  primary_exchange VARCHAR   NOT NULL,
  source           VARCHAR   NOT NULL DEFAULT 'openfigi',
  created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
