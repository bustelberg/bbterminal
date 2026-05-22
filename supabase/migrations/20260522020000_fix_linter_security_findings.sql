-- Fix Supabase linter "security" findings (the ones resolvable via SQL).
--
-- Findings addressed:
--   1. "Function Search Path Mutable" on 4 SQL helpers
--      (company_latest_close_price_dates, company_universe_labels,
--       universe_available_months, universe_all_companies_ever).
--   2. "Public/Signed-In Users Can Execute SECURITY DEFINER Function" on
--      public.set_admin_role_on_signup().
--   3. "Materialized View in API" on public.universe_stats.
--
-- Findings NOT in this migration (require Supabase dashboard action):
--   * "Leaked Password Protection Disabled" — toggle on at Authentication →
--     Settings → Password security.

-- ─── 1. Lock down function search_path ─────────────────────────────────
-- Same pattern the already-safe functions in the schema use
-- (SET search_path TO 'public', 'pg_temp'). Without this set, a caller can
-- prepend a malicious schema to search_path before invoking, causing
-- unqualified references like `metric_data` to resolve to attacker-owned
-- tables.
ALTER FUNCTION public.company_latest_close_price_dates()
    SET search_path TO 'public', 'pg_temp';
ALTER FUNCTION public.company_universe_labels()
    SET search_path TO 'public', 'pg_temp';
ALTER FUNCTION public.universe_available_months(integer)
    SET search_path TO 'public', 'pg_temp';
ALTER FUNCTION public.universe_all_companies_ever(integer)
    SET search_path TO 'public', 'pg_temp';

-- ─── 2. Lock down the SECURITY DEFINER trigger function ────────────────
-- PostgreSQL defaults to `GRANT EXECUTE ... TO PUBLIC` on functions, which
-- means anon/authenticated could `select set_admin_role_on_signup()` via the
-- REST API. The function is SECURITY DEFINER (runs as owner) so any direct
-- invocation would write raw_app_meta_data with elevated privileges —
-- specifically the email-matching branch could be probed.
-- Trigger firing doesn't require EXECUTE on the function (the trigger
-- machinery invokes it as the system), so revoking is safe.
REVOKE EXECUTE ON FUNCTION public.set_admin_role_on_signup() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.set_admin_role_on_signup() FROM anon, authenticated;

-- ─── 3. Hide universe_stats from PostgREST ─────────────────────────────
-- PostgREST auto-exposes materialized views in `public`. The frontend never
-- queries data via the anon key (only auth), so there's no reason for
-- anon/authenticated to read this MV. Backend uses service_role and
-- BYPASSRLS — its access is unaffected.
REVOKE SELECT ON public.universe_stats FROM anon, authenticated;

NOTIFY pgrst, 'reload schema';
