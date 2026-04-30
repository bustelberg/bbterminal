-- Address Supabase security linter findings on views + functions.
--
-- (1) Views: Postgres 15+ defaults views to running with the OWNER's
--     privileges (effectively SECURITY DEFINER). The Supabase linter
--     wants SECURITY INVOKER so RLS on the underlying tables applies
--     to the calling user. Set the storage option `security_invoker`
--     on each of our aggregate views.
--
-- (2) Functions: a function without an explicit search_path inherits
--     the caller's, which is a known privilege-escalation risk
--     (CVE-style: a malicious user with CREATE rights on another
--     schema could shadow built-in objects). Pin search_path to the
--     `public, pg_temp` pair so behaviour is deterministic regardless
--     of who calls.

ALTER VIEW IF EXISTS public.universe_stats           SET (security_invoker = on);
ALTER VIEW IF EXISTS public.universe_summary         SET (security_invoker = on);
ALTER VIEW IF EXISTS public.universe_sector_counts   SET (security_invoker = on);
ALTER VIEW IF EXISTS public.universe_monthly_counts  SET (security_invoker = on);

ALTER FUNCTION public.merge_company_data(integer, integer)
  SET search_path = public, pg_temp;
ALTER FUNCTION public.get_distinct_dates(text)
  SET search_path = public, pg_temp;
ALTER FUNCTION public.get_company_ids_for_date(text, date)
  SET search_path = public, pg_temp;
ALTER FUNCTION public.increment_api_usage(text, text, integer)
  SET search_path = public, pg_temp;
ALTER FUNCTION public.universe_full_stats()
  SET search_path = public, pg_temp;

NOTIFY pgrst, 'reload schema';
