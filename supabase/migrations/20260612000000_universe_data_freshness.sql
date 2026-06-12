-- Per-universe data-freshness for the /universe overview page.
--
-- Returns, for one universe: how many distinct companies have ever been a
-- member, how many of those have any GuruFocus data, and the most recent
-- close-price date we hold across them. Backs the per-universe freshness
-- readout + "Fetch data" decision on /universe.
--
-- `latest_date` is the max close_price date (NOT the max of all metrics):
-- close prices are daily, so this is a real "data current as of X" recency
-- signal — whereas analyst-estimate rows are future-dated and would always
-- report a far-future date.
--
-- Both per-member lookups (the close_price MAX and the has-any-data EXISTS)
-- resolve to a single seek on the metric_data primary key
-- (company_id, metric_code, source_code, target_date), so this stays fast even
-- for thousands-of-member universes — no extra index needed.

CREATE OR REPLACE FUNCTION public.universe_data_freshness(p_universe_id integer)
RETURNS TABLE(member_count integer, with_data integer, latest_date text)
    LANGUAGE sql STABLE
    AS $$
  WITH members AS (
    SELECT DISTINCT company_id
    FROM universe_membership
    WHERE universe_id = p_universe_id
  ),
  data AS (
    SELECT
      m.company_id,
      (SELECT MAX(md.target_date)
         FROM metric_data md
        WHERE md.company_id = m.company_id
          AND md.metric_code = 'close_price'
          AND md.source_code = 'gurufocus') AS last_price,
      EXISTS (
        SELECT 1 FROM metric_data md2
         WHERE md2.company_id = m.company_id
           AND md2.source_code = 'gurufocus'
      ) AS has_data
    FROM members m
  )
  SELECT
    (SELECT COUNT(*)::int FROM members),
    (SELECT COUNT(*)::int FROM data WHERE has_data),
    (SELECT MAX(last_price)::text FROM data);
$$;

GRANT EXECUTE ON FUNCTION public.universe_data_freshness(integer) TO anon, authenticated, service_role;
