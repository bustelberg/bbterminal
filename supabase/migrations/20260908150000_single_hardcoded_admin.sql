-- ⚠⚠ ONE HARDCODED ADMIN, NOT TWO (2026-09-08, on request). `reinier7175@gmail.com` keeps the
-- automatic grant; `reinier@bustelberg.nl` becomes an ordinary user.
--
-- ⚠ TWO HALVES, AND EITHER ALONE IS A NO-OP. Dropping the hash from the trigger only stops FUTURE
-- signups being promoted — the existing row carries an EXPLICIT `raw_app_meta_data.role = 'admin'`
-- written by 20260527010000's backfill, and `routers/auth.py::_resolve_role` returns an explicit
-- role in preference to the allowlist (deliberately: an explicit 'user' is an intentional
-- demotion). So the row has to be changed too, or nothing observable happens.
--
-- ⚠ A NEW MIGRATION RATHER THAN AN EDIT to 20260527010000 / 20260908090000. Both have already run
-- here and in production; editing an applied migration changes what the file SAYS without changing
-- what any database DID, which is how a schema and its history stop describing each other.
--
-- ⚠⚠ THIS LEAVES EXACTLY ONE ADMIN, AND THAT IS THE 2FA RECOVERY PATH GONE. Two admin accounts
-- were what let one reset the other's authenticator after a lost phone (`POST
-- /api/auth/users/{id}/mfa/reset`, which refuses self-service by design). With one, the remaining
-- routes back are: a SECOND authenticator enrolled on a different device, or deleting the factor
-- rows by hand in the Supabase SQL editor. Promoting the account again is one click on /users if
-- that trade turns out to be the wrong one.

CREATE OR REPLACE FUNCTION public.set_admin_role_on_signup() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO ''
    AS $$
DECLARE
  -- SHA-256 of lower(email). ⚠ MIRRORED in routers/auth.py::_ADMIN_EMAIL_HASHES and in the repair
  -- UPDATE below; tests/test_admin_email_hashes.py fails if the three drift apart.
  admin_hashes constant text[] := ARRAY[
    '5db5e75947119ef23451bc46919479a90b6bd51cd2e81815f2c7083e20fde36f'
  ];
BEGIN
  IF NEW.email IS NOT NULL
     AND encode(extensions.digest(lower(NEW.email), 'sha256'), 'hex') = ANY(admin_hashes)
  THEN
    NEW.raw_app_meta_data := COALESCE(NEW.raw_app_meta_data, '{}'::jsonb)
                          || jsonb_build_object('role', 'admin');
  END IF;
  RETURN NEW;
END;
$$;

-- The trigger itself is unchanged (20260908090000 attached it); replacing the function is enough.

-- ⚠ AN EXPLICIT 'user', NOT A REMOVED KEY. Deleting the role would leave the field absent, and an
-- absent role falls back to the hardcoded allowlist — which this address is no longer on, so it
-- would read as 'user' today and silently become admin again if anybody ever re-added the hash.
-- Writing 'user' states the decision instead of relying on an absence.
UPDATE auth.users
SET raw_app_meta_data = COALESCE(raw_app_meta_data, '{}'::jsonb)
                     || jsonb_build_object('role', 'user')
WHERE email IS NOT NULL
  AND encode(extensions.digest(lower(email), 'sha256'), 'hex')
      = '9fe083c7c1b2b6273a30b369870280d9cdfd3a89e165e6c2d68035cf1f7f144f'
  AND (raw_app_meta_data->>'role') IS DISTINCT FROM 'user';

-- Repair anything the remaining admin address should have and does not (idempotent; same statement
-- as 20260908090000's, narrowed to the one hash that still qualifies).
UPDATE auth.users
SET raw_app_meta_data = COALESCE(raw_app_meta_data, '{}'::jsonb)
                     || jsonb_build_object('role', 'admin')
WHERE email IS NOT NULL
  AND encode(extensions.digest(lower(email), 'sha256'), 'hex')
      = '5db5e75947119ef23451bc46919479a90b6bd51cd2e81815f2c7083e20fde36f'
  AND (raw_app_meta_data->>'role') IS DISTINCT FROM 'admin';
