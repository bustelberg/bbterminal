-- Restore Supabase's stock GRANTs on the public schema.
--
-- Symptom that prompted this: the FastAPI backend (using SUPABASE_SERVICE_KEY,
-- a valid service_role JWT) was getting `permission denied for table company`
-- (SQLSTATE 42501) on plain SELECTs. Investigation showed `service_role` had
-- BYPASSRLS = true but ZERO table grants on public.company — only `postgres`
-- was on the grant list. Same was true for every other public table.
--
-- Root cause: Supabase auto-grants SELECT/INSERT/UPDATE/DELETE on every newly
-- created public.* table to anon, authenticated, and service_role via an
-- event trigger. That trigger doesn't always fire when tables are created
-- through paths outside the normal migration flow (e.g. `db reset` against a
-- fresh project, raw DDL in Studio). When it misses, the table lands with
-- only the owner (`postgres`) granted — which is exactly what we saw.
--
-- BYPASSRLS doesn't help here: it only skips RLS *policy* checks, not the
-- table-level GRANT check. Both are gates and both must pass for non-owner
-- roles.
--
-- This migration brings the project back to Supabase's stock grant state:
--   * GRANT USAGE on schema public to the three Supabase-managed roles
--   * GRANT ALL on every existing table / sequence / function to all four
--     roles Supabase uses (postgres / anon / authenticated / service_role)
--   * ALTER DEFAULT PRIVILEGES so future tables/sequences/functions in
--     public.* inherit the same grants automatically — independent of
--     whether Supabase's event trigger fires.
--
-- Granting SELECT to anon/authenticated is safe because RLS is enabled on
-- every public table with zero policies (see 20260501 in git history; the
-- consolidated initial_schema preserves the RLS-on state). Without a
-- passing policy, anon/authenticated still hit default-deny at the policy
-- layer. service_role unlocks because BYPASSRLS short-circuits the policy
-- check once the GRANT is in place.

GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;

GRANT ALL ON ALL TABLES    IN SCHEMA public TO postgres, anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO postgres, anon, authenticated, service_role;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA public TO postgres, anon, authenticated, service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL ON TABLES    TO postgres, anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL ON SEQUENCES TO postgres, anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL ON FUNCTIONS TO postgres, anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
