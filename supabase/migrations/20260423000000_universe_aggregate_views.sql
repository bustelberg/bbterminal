-- One-shot universe stats: a single RPC returning all aggregates per universe
-- so /api/universe/labels needs just two round trips total (universe + stats),
-- regardless of how many universes or membership rows exist.

CREATE OR REPLACE VIEW universe_monthly_counts AS
SELECT
  universe_id,
  target_month,
  COUNT(*)::int AS member_count
FROM universe_membership
GROUP BY universe_id, target_month;

CREATE OR REPLACE VIEW universe_sector_counts AS
SELECT
  universe_id,
  COALESCE(NULLIF(sector, ''), '(unknown)') AS sector,
  COUNT(*)::int AS member_count
FROM universe_membership
GROUP BY universe_id, COALESCE(NULLIF(sector, ''), '(unknown)');

CREATE OR REPLACE VIEW universe_summary AS
SELECT
  universe_id,
  COUNT(*)::int                          AS total_rows,
  COUNT(DISTINCT company_id)::int        AS unique_companies,
  COUNT(DISTINCT universe_ticker)::int   AS unique_tickers,
  COUNT(DISTINCT target_month)::int      AS month_count,
  MIN(target_month)                      AS start_month,
  MAX(target_month)                      AS end_month
FROM universe_membership
GROUP BY universe_id;

CREATE OR REPLACE FUNCTION universe_full_stats()
RETURNS TABLE (
  universe_id       int,
  total_rows        int,
  unique_companies  int,
  unique_tickers    int,
  month_count       int,
  start_month       text,
  end_month         text,
  monthly_counts    jsonb,
  sector_counts     jsonb
) LANGUAGE sql STABLE AS $$
  WITH monthly AS (
    SELECT
      universe_id,
      jsonb_agg(
        jsonb_build_object('month', target_month, 'count', member_count)
        ORDER BY target_month
      ) AS arr
    FROM universe_monthly_counts
    GROUP BY universe_id
  ),
  sectors AS (
    SELECT
      universe_id,
      jsonb_agg(
        jsonb_build_object('sector', sector, 'count', member_count)
        ORDER BY member_count DESC
      ) AS arr
    FROM universe_sector_counts
    GROUP BY universe_id
  )
  SELECT
    s.universe_id,
    s.total_rows,
    s.unique_companies,
    s.unique_tickers,
    s.month_count,
    s.start_month::text,
    s.end_month::text,
    COALESCE(m.arr, '[]'::jsonb) AS monthly_counts,
    COALESCE(sec.arr, '[]'::jsonb) AS sector_counts
  FROM universe_summary s
  LEFT JOIN monthly m USING (universe_id)
  LEFT JOIN sectors sec USING (universe_id);
$$;
