-- RPC for the admin endpoint `/api/admin/companies/flagged`. Two
-- ad-hoc heuristics that bubble up companies worth manual review:
--
--   * `company_flat_price_run(window_days)` — companies whose latest
--     `window_days` close_price observations are all the exact same
--     value. Strong signal for stale/dead listings or for wrong
--     dual-listing mappings (e.g. mapping the primary security to an
--     inactive ADR whose stub price never moves).
--
-- ADR-name detection isn't an RPC — it's a single `ILIKE '%ADR%'`
-- the backend runs directly.
--
-- STABLE so PostgREST + the query planner can cache; SECURITY INVOKER
-- (the default) so RLS still applies under the caller's role.

CREATE OR REPLACE FUNCTION company_flat_price_run(window_days int DEFAULT 10)
RETURNS TABLE (
    company_id int,
    flat_value double precision,
    window_start text,
    window_end text,
    row_count bigint
)
LANGUAGE sql
STABLE
AS $$
    WITH last_n AS (
        SELECT
            md.company_id,
            md.target_date,
            md.numeric_value,
            ROW_NUMBER() OVER (
                PARTITION BY md.company_id
                ORDER BY md.target_date DESC
            ) AS rn
        FROM metric_data md
        WHERE md.metric_code = 'close_price'
          AND md.numeric_value IS NOT NULL
    )
    SELECT
        last_n.company_id::int,
        MAX(last_n.numeric_value)::double precision AS flat_value,
        MIN(last_n.target_date)::text AS window_start,
        MAX(last_n.target_date)::text AS window_end,
        COUNT(*) AS row_count
    FROM last_n
    WHERE last_n.rn <= window_days
    GROUP BY last_n.company_id
    HAVING COUNT(*) >= window_days
       AND COUNT(DISTINCT last_n.numeric_value) = 1;
$$;

GRANT EXECUTE ON FUNCTION company_flat_price_run(int) TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
