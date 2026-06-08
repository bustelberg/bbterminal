-- Security hardening (audit finding M3): stop `anon` / `authenticated`
-- from being able to WRITE to public tables directly via PostgREST.
--
-- Background: 20260522000000_restore_supabase_default_grants.sql granted
-- ALL privileges (incl. INSERT/UPDATE/DELETE) on every current AND future
-- public.* table to anon + authenticated, which makes RLS the SOLE gate.
-- That holds only as long as every table keeps RLS enabled — a single
-- future table created without `ENABLE ROW LEVEL SECURITY` would be
-- world-WRITABLE via `curl {SUPABASE_URL}/rest/v1/<table>` with the public
-- anon key, bypassing the FastAPI admin middleware entirely.
--
-- Why this is safe (no app behavior change):
--   * the backend writes as service_role (BYPASSRLS, keeps ALL grants);
--   * the frontend uses the anon key only for auth.* — it never calls
--     PostgREST `.from(<table>)` directly.
-- So removing write grants from anon/authenticated just removes the latent
-- foot-gun. SELECT is intentionally left in place (RLS deny-all already
-- blocks reads); the CI "RLS on every public table" guard in
-- .github/workflows/ci.yml is the belt that fails any future no-RLS table.

-- Existing tables. (REVOKE of a privilege a role doesn't hold is a no-op,
-- so this is safe even where the grant was never present.)
REVOKE INSERT, UPDATE, DELETE, TRUNCATE
  ON ALL TABLES IN SCHEMA public
  FROM anon, authenticated;

-- Future tables — override the GRANT ALL default set by 20260522000000 so
-- newly created tables inherit read-only (at most) for anon/authenticated.
-- Runs in the same role context as that migration, so it targets the same
-- default-privilege set.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLES
  FROM anon, authenticated;

NOTIFY pgrst, 'reload schema';
