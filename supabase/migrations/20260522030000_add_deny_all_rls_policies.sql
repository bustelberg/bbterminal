-- Silence the "RLS Enabled No Policy" Supabase linter advisory by adding an
-- explicit deny-all policy on every public.* table that has RLS on but no
-- policies.
--
-- Behavior change: NONE. With RLS on and zero policies, the default is
-- already "deny-all to non-BYPASSRLS roles" (i.e. anon/authenticated see no
-- rows; service_role bypasses entirely via BYPASSRLS). Adding `USING (false)
-- WITH CHECK (false)` just documents that intent in pg_policies so the linter
-- knows the configuration is deliberate.
--
-- service_role has BYPASSRLS, so this policy doesn't apply to it. The FastAPI
-- backend (which uses service_role) continues to read/write normally.
-- anon and authenticated stay default-deny — same as today, just explicit.
--
-- Idempotent: skips any table that already has a policy. Run again safely if
-- new tables get added; you'll catch them on the next sweep.

DO $$
DECLARE
  t record;
BEGIN
  FOR t IN
    SELECT schemaname, tablename
    FROM pg_tables
    WHERE schemaname = 'public'
      AND rowsecurity = true
      -- Skip tables that already have at least one policy.
      AND NOT EXISTS (
        SELECT 1 FROM pg_policies p
        WHERE p.schemaname = pg_tables.schemaname
          AND p.tablename = pg_tables.tablename
      )
  LOOP
    EXECUTE format(
      'CREATE POLICY %I ON %I.%I AS RESTRICTIVE FOR ALL TO PUBLIC USING (false) WITH CHECK (false)',
      'deny_all_non_service_role', t.schemaname, t.tablename
    );
  END LOOP;
END $$;

NOTIFY pgrst, 'reload schema';
