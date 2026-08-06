-- GuruFocus price/volume coverage, per company — so `/asset-pipeline` can finally show the
-- OTHER vendor's series beside Yahoo's, and "do we have GuruFocus prices for this?" stops being
-- a question only a query can answer.
--
-- ⚠⚠ A MATERIALIZED VIEW, NOT A LATERAL ON `asset_grid`. `metric_data` is ~36M rows — a plain
--   `COUNT(*)` on it TIMES OUT through PostgREST (57014). `asset_grid` is ~16,150 rows, so a
--   LATERAL aggregate would run that scan 16,150 times per page load. Aggregated ONCE here and
--   joined by `company_id` (2,790 rows, unique index) it is free at read time.
--
-- ⚠ THE PRICE CODE IS `close_price`, NOT `price`. Nothing enforces this vocabulary and the
--   obvious guess returns zero rows while looking exactly like "this company has no prices" —
--   measured: SMIC read 0 under `metric_code='price'` and 5,524 under `close_price`. That silent
--   empty is the entire reason these columns are worth surfacing: a wrong code and a real gap are
--   indistinguishable from the outside.
--
-- ⚠ `metric_data` IS NOT A PRICE TABLE. It also holds every fundamentals line the earnings
--   pipeline writes (`annuals__Balance Sheet__…`, the estimate series, …) — which is what makes it
--   36M rows. The WHERE clause is load-bearing: without it this aggregates statement lines into a
--   "bar count" that would be nonsense, and scans the whole table to do it.
--
-- ⚠ COUNTS ARE PER METRIC, KEPT SEPARATE. Price and volume are written by the same fetch and
--   normally match exactly (SMIC: 5,524 and 5,524), but they are separate upserts with separate
--   retry paths, so a divergence is real information about a half-completed refresh. Collapsing
--   them to one "bars" number would hide precisely the case worth seeing.
--
-- ⚠ `_from`/`_to` ARE `target_date`, THE TRADING DAY — never `recorded_at`. This table is
--   append-only in `recorded_at` but NOT in `target_date`: GuruFocus publishes some closes late
--   and the ingest writes them under their true, earlier date (Bayer's 2026-07-03 close landed
--   2026-07-06). `max(target_date)` is therefore the honest "how current is this series", which is
--   the question being asked; `max(recorded_at)` would answer "when did we last touch it".

DROP MATERIALIZED VIEW IF EXISTS public.company_price_coverage;
CREATE MATERIALIZED VIEW public.company_price_coverage AS
SELECT
    company_id,
    min(target_date) FILTER (WHERE metric_code = 'close_price') AS price_from,
    max(target_date) FILTER (WHERE metric_code = 'close_price') AS price_to,
    count(*)         FILTER (WHERE metric_code = 'close_price') AS price_bars,
    min(target_date) FILTER (WHERE metric_code = 'volume')      AS volume_from,
    max(target_date) FILTER (WHERE metric_code = 'volume')      AS volume_to,
    count(*)         FILTER (WHERE metric_code = 'volume')      AS volume_bars
  FROM public.metric_data
 WHERE metric_code IN ('close_price', 'volume')
 GROUP BY company_id;

-- UNIQUE so the refresh can run CONCURRENTLY — without it every refresh takes an
-- ACCESS EXCLUSIVE lock and readers block on a ~36M-row scan.
CREATE UNIQUE INDEX company_price_coverage_pk
    ON public.company_price_coverage (company_id);

REVOKE ALL ON public.company_price_coverage FROM anon, authenticated;
GRANT SELECT ON public.company_price_coverage TO service_role;


-- The refresh, callable over PostgREST (which cannot issue REFRESH itself).
--
-- ⚠ CONCURRENTLY, so the price phase never blocks the app mid-run. It needs the unique index
--   above and cannot run inside a transaction block — hence a plain function, not a DO block.
-- ⚠ SECURITY DEFINER because the materialized view is owned by the migration role; the caller
--   (service_role) may refresh it but does not own it. `search_path` is pinned so the function
--   cannot be redirected by a caller-supplied path.
CREATE OR REPLACE FUNCTION public.refresh_company_price_coverage()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.company_price_coverage;
END;
$$;

REVOKE ALL ON FUNCTION public.refresh_company_price_coverage() FROM anon, authenticated;
GRANT EXECUTE ON FUNCTION public.refresh_company_price_coverage() TO service_role;

NOTIFY pgrst, 'reload schema';
