-- Per-month LongEquity membership, derived from the monthly report data.
--
-- LongEquity is the one time-series universe (is_monthly=true). Its membership
-- should be TRUE point-in-time: each month = the companies that actually
-- appear in that month's LongEquity report. The source of truth is
-- metric_data rows with source_code='longequity' (one report date per month).
--
-- Returns one row per report month with the distinct company_ids for that
-- month as an int[] — so the whole panel comes back in ~9 rows (one per
-- month), staying well under the PostgREST 1000-row cap regardless of how
-- many companies/months exist (a flat (month, company_id) projection would
-- be thousands of rows and silently truncate in prod).
CREATE OR REPLACE FUNCTION public.longequity_membership_by_month()
RETURNS TABLE(target_month text, company_ids integer[])
LANGUAGE sql
STABLE
AS $$
  SELECT to_char(target_date, 'YYYY-MM') AS target_month,
         array_agg(DISTINCT company_id ORDER BY company_id) AS company_ids
    FROM public.metric_data
   WHERE source_code = 'longequity'
   GROUP BY to_char(target_date, 'YYYY-MM')
   ORDER BY 1;
$$;
