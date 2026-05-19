-- DB-side aggregation function for the "all companies ever in this
-- universe" CSV export. Doing this in Postgres (one round-trip + one
-- GROUP BY) instead of paginating ~290 months × ~3000 holdings into
-- Python and aggregating in memory turns a ~10s endpoint into ~500ms.
--
-- Used by `UniverseTemplate.all_companies_ever()` → `GET
-- /api/universe-templates/{key}/all-companies.csv`.
--
-- For each company that has ever been in the given universe:
--   gurufocus_ticker   — canonical company ticker
--   exchange_code      — GuruFocus exchange code (e.g. "NYSE", "LSE")
--   exchange_name      — full human-readable exchange name
--   company_name       — `company.company_name`
--   sector             — most recent sector seen in the universe
--   gurufocus_url      — built in Python from ticker + exchange_code
--
-- (Aggregation fields kept for callers that want first/last month +
-- still-current flag, but the CSV exporter today writes only the six
-- columns above.)

-- `CREATE OR REPLACE` can't change return-type shape; drop first so
-- the migration is idempotent if the signature evolves later.
DROP FUNCTION IF EXISTS universe_all_companies_ever(INTEGER);

CREATE OR REPLACE FUNCTION universe_all_companies_ever(p_universe_id INTEGER)
RETURNS TABLE(
  company_id        INTEGER,
  gurufocus_ticker  TEXT,
  exchange_code     TEXT,
  exchange_name     TEXT,
  company_name      TEXT,
  sector            TEXT,
  first_month       TEXT,
  last_month        TEXT,
  months_count      INTEGER,
  still_current     BOOLEAN
)
LANGUAGE sql
STABLE
AS $$
  WITH agg AS (
    SELECT
      um.company_id,
      MIN(um.target_month) AS first_month,
      MAX(um.target_month) AS last_month,
      COUNT(DISTINCT um.target_month)::INTEGER AS months_count
    FROM universe_membership um
    WHERE um.universe_id = p_universe_id
    GROUP BY um.company_id
  ),
  -- Single scalar: the latest month this universe has any membership row
  -- for. Used to flag `still_current`.
  latest_month AS (
    SELECT MAX(target_month) AS m
    FROM universe_membership
    WHERE universe_id = p_universe_id
  ),
  -- For each company, take the most-recent universe_membership row.
  -- DISTINCT ON pattern is Postgres's idiomatic "top-1 per group".
  latest_per_company AS (
    SELECT DISTINCT ON (um.company_id)
      um.company_id,
      um.sector
    FROM universe_membership um
    WHERE um.universe_id = p_universe_id
    ORDER BY um.company_id, um.target_month DESC
  )
  SELECT
    a.company_id,
    c.gurufocus_ticker,
    ge.exchange_code,
    ge.exchange_name,
    c.company_name,
    l.sector,
    a.first_month,
    a.last_month,
    a.months_count,
    (a.last_month = (SELECT m FROM latest_month)) AS still_current
  FROM agg a
  JOIN latest_per_company l ON l.company_id = a.company_id
  LEFT JOIN company c ON c.company_id = a.company_id
  LEFT JOIN gurufocus_exchange ge ON ge.exchange_id = c.exchange_id
  ORDER BY c.gurufocus_ticker NULLS LAST;
$$;

NOTIFY pgrst, 'reload schema';
