-- Restore the second hardcoded administrator. The prior migration reduced the list to one; this
-- is a NEW migration because editing an already-applied migration would not change production.
-- The list is SHA-256(lower(email)) so no administrator address is stored in source.
--
-- This changes all three parts together: the trigger for future signups, this repair for existing
-- user metadata, and `backend/routers/auth.py::_ADMIN_EMAIL_HASHES` for backend fallback checks.

CREATE OR REPLACE FUNCTION public.set_admin_role_on_signup() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO ''
    AS $$
DECLARE
  admin_hashes constant text[] := ARRAY[
    '5db5e75947119ef23451bc46919479a90b6bd51cd2e81815f2c7083e20fde36f',
    '9fe083c7c1b2b6273a30b369870280d9cdfd3a89e165e6c2d68035cf1f7f144f'
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

-- The restored account may already exist with an explicit `role: user` from the prior migration.
-- Promote it so its next authenticated session and the frontend agree with the backend.
UPDATE auth.users
SET raw_app_meta_data = COALESCE(raw_app_meta_data, '{}'::jsonb)
                     || jsonb_build_object('role', 'admin')
WHERE email IS NOT NULL
  AND encode(extensions.digest(lower(email), 'sha256'), 'hex')
      = '9fe083c7c1b2b6273a30b369870280d9cdfd3a89e165e6c2d68035cf1f7f144f'
  AND (raw_app_meta_data->>'role') IS DISTINCT FROM 'admin';
