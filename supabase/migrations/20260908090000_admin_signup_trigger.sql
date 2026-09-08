-- ⚠⚠ THE ADMIN-ON-SIGNUP FUNCTION EXISTED FOR MONTHS AND WAS ATTACHED TO NOTHING.
--
-- `public.set_admin_role_on_signup()` is created in 20260101000000 and rewritten in
-- 20260527010000 (plaintext emails → SHA-256 hashes), and BOTH migrations create only the
-- FUNCTION. `grep -rn "CREATE TRIGGER" supabase/migrations` matches nothing on `auth.users`:
-- there has never been a trigger calling it. `supabase db diff`/`db dump` do not cover the `auth`
-- schema, so a trigger created by hand in the dashboard is invisible to this directory and a
-- local `db reset` recreates the database without it — which is what `pg_trigger` says about the
-- local database right now (`triggers on auth.users: NONE`).
--
-- ⚠ WHAT MADE IT LOOK FINE. Both admin accounts DO carry `role=admin` locally — from the one-shot
-- backfill UPDATE at the bottom of 20260527010000, which ran once when that migration was applied.
-- A backfill is not a rule: sign the same address up again (a deleted account, a fresh
-- environment) and the new row gets `raw_app_meta_data->>'role' = NULL`, with nothing to set it.
--
-- ⚠⚠ AND THE TWO HALVES OF THE APP THEN DISAGREE, WHICH IS WORSE THAN BEING PLAINLY LOCKED OUT.
-- The backend's `_resolve_role` falls back to the same hash list, so the API answers as ADMIN;
-- the frontend reads `app_metadata.role` verbatim (`Sidebar`: `meta.role === 'admin'`), so it
-- renders the regular-user nav. The account is admin to every endpoint and a user to every screen
-- — the exact desync `_resolve_role`'s own docstring warns about, reached from the other side.
--
-- ⚠ BEFORE INSERT, because the function assigns to NEW. An AFTER trigger would return a row
-- nobody reads and change nothing.
--
-- ⚠ NON-ADMINS ARE DELIBERATELY LEFT ALONE — the function only writes the key when the hash
-- matches, so everyone else keeps `role` ABSENT rather than an explicit 'user'. That is load-
-- bearing: `_resolve_role` treats an explicit 'user' as an INTENTIONAL demotion that outranks the
-- hardcoded allowlist, so writing 'user' for everybody would permanently pin an admin whose role
-- was later wiped. Absent reads as 'user' on both sides already.
--
-- ⚠ The hash list lives in THREE places and they move together: this function,
-- `backend/routers/auth.py::_ADMIN_EMAIL_HASHES`, and the backfill below. Pinned by
-- `backend/tests/test_admin_email_hashes.py`.

DROP TRIGGER IF EXISTS set_admin_role_on_signup ON auth.users;

CREATE TRIGGER set_admin_role_on_signup
    BEFORE INSERT ON auth.users
    FOR EACH ROW
    EXECUTE FUNCTION public.set_admin_role_on_signup();

-- Repair anything that signed up while the trigger was missing. Same statement as
-- 20260527010000's, and it has to be repeated rather than relied upon: that one ran against the
-- rows existing THEN, and every account created since is exactly the set this migration exists
-- for. Idempotent — the `IS DISTINCT FROM` clause means a second run touches nothing.
UPDATE auth.users
SET raw_app_meta_data = COALESCE(raw_app_meta_data, '{}'::jsonb)
                     || jsonb_build_object('role', 'admin')
WHERE email IS NOT NULL
  AND encode(extensions.digest(lower(email), 'sha256'), 'hex') = ANY(ARRAY[
    '9fe083c7c1b2b6273a30b369870280d9cdfd3a89e165e6c2d68035cf1f7f144f',
    '5db5e75947119ef23451bc46919479a90b6bd51cd2e81815f2c7083e20fde36f'
  ])
  AND (raw_app_meta_data->>'role') IS DISTINCT FROM 'admin';
