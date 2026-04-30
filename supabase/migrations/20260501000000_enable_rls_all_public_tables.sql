-- Enable Row Level Security on every table in the public schema.
--
-- Context: the FastAPI backend uses the SERVICE_ROLE key, which bypasses
-- RLS by design (Supabase grants service_role the BYPASSRLS attribute).
-- The Next.js frontend only uses Supabase for *auth* — it never queries
-- application tables directly via the anon key — so locking down the
-- anon path is safe.
--
-- Why no policies: with RLS enabled and zero policies, the default deny
-- applies for any role *without* BYPASSRLS. That's the lockdown the
-- Supabase linter is asking for. If we ever want frontend reads via
-- the anon key, we'd add explicit `CREATE POLICY` statements per table.
--
-- This migration is idempotent: ENABLE RLS on a table that already has
-- it on is a no-op. Loops over public.tables so we don't have to keep
-- this list in sync as schema evolves.

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
