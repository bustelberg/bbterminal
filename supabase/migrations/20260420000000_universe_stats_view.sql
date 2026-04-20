-- Precomputed per-universe aggregates for the /momentum universe dropdown.
-- The old endpoint paged through every universe_membership row just to compute
-- month/ticker counts in Python, which cost ~70s for S&P 500 + ACWI.
CREATE OR REPLACE VIEW universe_stats AS
SELECT
  u.universe_id,
  u.label,
  u.description,
  u.created_at,
  MIN(m.target_month)                     AS start_month,
  MAX(m.target_month)                     AS end_month,
  COUNT(DISTINCT m.target_month)          AS month_count,
  COUNT(DISTINCT m.universe_ticker)       AS total_unique_tickers
FROM universe u
LEFT JOIN universe_membership m ON m.universe_id = u.universe_id
GROUP BY u.universe_id, u.label, u.description, u.created_at;
