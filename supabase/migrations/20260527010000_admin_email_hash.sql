-- Swap plaintext-email admin trigger for a SHA-256 hash check so admin
-- emails no longer appear in source. Backfills any existing matching
-- user that doesn't already have role=admin.

CREATE OR REPLACE FUNCTION public.set_admin_role_on_signup() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO ''
    AS $$
DECLARE
  admin_hashes constant text[] := ARRAY[
    '9fe083c7c1b2b6273a30b369870280d9cdfd3a89e165e6c2d68035cf1f7f144f',
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

UPDATE auth.users
SET raw_app_meta_data = COALESCE(raw_app_meta_data, '{}'::jsonb)
                     || jsonb_build_object('role', 'admin')
WHERE email IS NOT NULL
  AND encode(extensions.digest(lower(email), 'sha256'), 'hex') = ANY(ARRAY[
    '9fe083c7c1b2b6273a30b369870280d9cdfd3a89e165e6c2d68035cf1f7f144f',
    '5db5e75947119ef23451bc46919479a90b6bd51cd2e81815f2c7083e20fde36f'
  ])
  AND (raw_app_meta_data->>'role') IS DISTINCT FROM 'admin';
