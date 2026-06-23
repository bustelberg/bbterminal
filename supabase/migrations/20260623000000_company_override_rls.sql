-- Enable RLS + a deny-all policy on company_override.
--
-- The table was added (20260617002000) without RLS. Supabase's default
-- privileges grant anon/authenticated access to new public tables, so without
-- RLS the anon key could read/write company_override via PostgREST — flagged by
-- the public-table RLS CI check. This follows the project's deny-all default:
-- RLS on + a `USING (false)` policy means anon/authenticated see nothing, while
-- the backend's service_role bypasses RLS (BYPASSRLS) and keeps reading/writing
-- it from the ingest override pass. Mirrors panel_cache / fee_config.
--
-- Idempotent: ENABLE RLS is a no-op if already on; the policy is dropped first.

ALTER TABLE public.company_override ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS company_override_deny_all ON public.company_override;
CREATE POLICY company_override_deny_all ON public.company_override FOR ALL USING (false);

GRANT SELECT, INSERT, UPDATE, DELETE ON public.company_override TO service_role;

NOTIFY pgrst, 'reload schema';
