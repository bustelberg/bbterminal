-- Yahoo is the sole market-data vendor for company prices and volumes.
--
-- The product domain still identifies equities by `company_id`; Yahoo bars are
-- keyed by `analysis_id`. This view is the canonical, read-only bridge. It
-- deliberately joins through the exact ISIN execution chosen by the reviewed
-- resolver, so no caller can accidentally price a company from its
-- GuruFocus ticker or a similarly named foreign listing.

CREATE OR REPLACE VIEW public.company_yahoo_price AS
SELECT
    c.company_id,
    e.analysis_id,
    e.currency,
    p.target_date,
    p.close,
    p.volume
FROM public.company c
JOIN public.asset_execution e
  ON e.isin = c.isin
 AND e.status = 'ok'
 AND e.analysis_id IS NOT NULL
JOIN public.asset_price p
  ON p.analysis_id = e.analysis_id
WHERE p.close IS NOT NULL;

COMMENT ON VIEW public.company_yahoo_price IS
  'Canonical company_id-to-Yahoo close/volume bridge. Replaces GuruFocus metric_data close_price and volume reads.';

REVOKE ALL ON public.company_yahoo_price FROM anon, authenticated;
GRANT SELECT ON public.company_yahoo_price TO service_role;

CREATE OR REPLACE FUNCTION public.company_yahoo_latest_dates_for(p_company_ids integer[])
RETURNS TABLE(company_id integer, latest_close_date date, latest_volume_date date)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT yp.company_id,
         max(yp.target_date) AS latest_close_date,
         max(yp.target_date) FILTER (WHERE yp.volume IS NOT NULL) AS latest_volume_date
    FROM public.company_yahoo_price yp
   WHERE yp.company_id = ANY(p_company_ids)
   GROUP BY yp.company_id;
$$;

REVOKE ALL ON FUNCTION public.company_yahoo_latest_dates_for(integer[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.company_yahoo_latest_dates_for(integer[]) TO service_role;

CREATE OR REPLACE FUNCTION public.company_yahoo_latest_close_dates()
RETURNS TABLE(company_id integer, latest_target_date text)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public
AS $$
  SELECT yp.company_id, max(yp.target_date)::text
  FROM public.company_yahoo_price yp GROUP BY yp.company_id;
$$;
REVOKE ALL ON FUNCTION public.company_yahoo_latest_close_dates() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.company_yahoo_latest_close_dates() TO service_role;

CREATE OR REPLACE FUNCTION public.company_yahoo_coverage_for(
    p_company_ids integer[], p_series text, p_since date)
RETURNS TABLE(company_id integer, earliest_target_date text,
              latest_target_date text, points_since integer, max_gap_days integer)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public
AS $$
  WITH rows AS (
    SELECT yp.company_id, yp.target_date FROM public.company_yahoo_price yp
    WHERE yp.company_id = ANY(p_company_ids)
      AND (p_series = 'close' OR (p_series = 'volume' AND yp.volume IS NOT NULL))
  ), spans AS (
    SELECT company_id, min(target_date) earliest, max(target_date) latest
    FROM rows GROUP BY company_id
  ), windowed AS (
    SELECT company_id, target_date,
      target_date - lag(target_date) OVER (PARTITION BY company_id ORDER BY target_date) gap
    FROM rows WHERE target_date >= p_since
  ), wagg AS (
    SELECT company_id, count(*) points, coalesce(max(gap), 0) maxgap
    FROM windowed GROUP BY company_id
  )
  SELECT s.company_id, s.earliest::text, s.latest::text,
         coalesce(w.points, 0)::integer, coalesce(w.maxgap, 0)::integer
  FROM spans s LEFT JOIN wagg w USING (company_id);
$$;
REVOKE ALL ON FUNCTION public.company_yahoo_coverage_for(integer[], text, date) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.company_yahoo_coverage_for(integer[], text, date) TO service_role;

NOTIFY pgrst, 'reload schema';
