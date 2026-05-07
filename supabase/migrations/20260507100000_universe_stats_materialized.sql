-- Convert `universe_stats` from a regular view to a materialized view.
-- The original view did `COUNT(DISTINCT universe_ticker)` over the full
-- universe_membership table on every read. For the typical SP500 + ACWI
-- footprint that's ~500k rows of hash aggregation per request and started
-- tripping Supabase's 8-second statement_timeout (postgrest error 57014),
-- which broke the /momentum universe dropdown.
--
-- A materialized view persists the aggregates to disk; reads become a
-- straight scan of the materialized rows (a few hundred rows max) instead
-- of an aggregate over hundreds of thousands. The trade-off is staleness:
-- the materialized view doesn't auto-update when universe_membership
-- changes. Refresh it after an ingest with:
--
--     REFRESH MATERIALIZED VIEW CONCURRENTLY universe_stats;
--
-- The CONCURRENTLY clause needs the unique index below. Without it
-- (or on the very first refresh), use plain `REFRESH MATERIALIZED VIEW
-- universe_stats;`, which briefly locks readers but completes in seconds.

DROP VIEW IF EXISTS universe_stats CASCADE;

CREATE MATERIALIZED VIEW universe_stats AS
SELECT
  u.universe_id,
  u.label,
  u.description,
  u.created_at,
  MIN(m.target_month)               AS start_month,
  MAX(m.target_month)               AS end_month,
  COUNT(DISTINCT m.target_month)    AS month_count,
  COUNT(DISTINCT m.universe_ticker) AS total_unique_tickers
FROM universe u
LEFT JOIN universe_membership m ON m.universe_id = u.universe_id
GROUP BY u.universe_id, u.label, u.description, u.created_at;

-- CONCURRENTLY refresh requires a unique index. universe_id is the
-- natural key (label is also unique but universe_id is the FK target
-- elsewhere, so keep it primary).
CREATE UNIQUE INDEX IF NOT EXISTS universe_stats_universe_id_uniq
  ON universe_stats (universe_id);

-- Initial population so the first read after this migration has data.
REFRESH MATERIALIZED VIEW universe_stats;

-- security_invoker isn't valid on materialized views (they're plain
-- relations, not views), so the earlier ALTER VIEW SECURITY INVOKER on
-- this name no longer applies — that's expected and intentional.

NOTIFY pgrst, 'reload schema';
