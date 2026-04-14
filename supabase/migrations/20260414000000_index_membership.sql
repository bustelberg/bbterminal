-- Index membership: stores which tickers belong to an index each month
CREATE TABLE IF NOT EXISTS index_membership (
  index_name    TEXT    NOT NULL,
  target_month  TEXT    NOT NULL,
  ticker        TEXT    NOT NULL,
  company_id    INTEGER REFERENCES company(company_id) ON DELETE SET NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (index_name, target_month, ticker)
);

CREATE INDEX IF NOT EXISTS idx_index_membership_index_month
  ON index_membership (index_name, target_month);

-- List all stored indexes with month range and unique ticker counts
CREATE OR REPLACE FUNCTION index_membership_indexes()
RETURNS TABLE(index_name TEXT, start_month TEXT, end_month TEXT, month_count BIGINT, total_unique_tickers BIGINT) AS $$
  SELECT
    index_name,
    MIN(target_month) AS start_month,
    MAX(target_month) AS end_month,
    COUNT(DISTINCT target_month) AS month_count,
    COUNT(DISTINCT ticker) AS total_unique_tickers
  FROM index_membership
  GROUP BY index_name
  ORDER BY index_name;
$$ LANGUAGE sql STABLE;

-- All unique tickers for an index, preferring rows that have a company_id
CREATE OR REPLACE FUNCTION index_membership_cumulative(p_index TEXT)
RETURNS TABLE(ticker TEXT, company_id INTEGER) AS $$
  SELECT DISTINCT ON (ticker)
    ticker,
    company_id
  FROM index_membership
  WHERE index_name = p_index
  ORDER BY ticker, company_id NULLS LAST;
$$ LANGUAGE sql STABLE;

-- List months for a given index with ticker counts
CREATE OR REPLACE FUNCTION index_membership_months(p_index TEXT)
RETURNS TABLE(month TEXT, ticker_count BIGINT) AS $$
  SELECT
    target_month AS month,
    COUNT(*) AS ticker_count
  FROM index_membership
  WHERE index_name = p_index
  GROUP BY target_month
  ORDER BY target_month;
$$ LANGUAGE sql STABLE;
