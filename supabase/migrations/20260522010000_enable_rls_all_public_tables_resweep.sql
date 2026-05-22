-- Re-run of the RLS-everything sweep.
--
-- The original 20260501 migration enabled RLS on every public.* table that
-- existed at the time. Tables added afterward (currently: public.leonteq_equity
-- from the Leonteq integration, flagged by the Supabase linter as "RLS
-- Disabled in Public") missed it. The fix is the same idempotent DO-block:
-- ENABLE RLS on an already-enabled table is a no-op, so this sweep safely
-- covers leonteq_equity now and any future table the original migration
-- couldn't have known about.
--
-- Why we accept no policies: backend uses service_role (BYPASSRLS); frontend
-- only uses Supabase for auth and never queries application tables. With RLS
-- on and zero policies, anon/authenticated default-deny — the lockdown
-- Supabase linter wants.
--
-- If a future table needs frontend-readable access via the anon key, add an
-- explicit `CREATE POLICY` in its own migration.

DO $$
DECLARE
  t record;
BEGIN
  FOR t IN
    SELECT schemaname, tablename
    FROM pg_tables
    WHERE schemaname = 'public'
  LOOP
    EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY', t.schemaname, t.tablename);
  END LOOP;
END $$;

NOTIFY pgrst, 'reload schema';
