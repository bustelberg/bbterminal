-- Per-company universe labels in one shot. /api/companies needs to know which
-- universes each company appears in; doing this in Python by paginating
-- universe_membership (one row per (universe, company, month)) was 30+ pages
-- and several seconds. This RPC aggregates server-side and returns one row
-- per company, with a sorted distinct array of labels.

CREATE OR REPLACE FUNCTION company_universe_labels()
RETURNS TABLE (
  company_id int,
  labels     text[]
) LANGUAGE sql STABLE AS $$
  SELECT
    m.company_id,
    array_agg(DISTINCT u.label ORDER BY u.label) AS labels
  FROM universe_membership m
  JOIN universe u USING (universe_id)
  GROUP BY m.company_id;
$$;
