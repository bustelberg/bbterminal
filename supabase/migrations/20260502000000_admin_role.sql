-- Admin role lives in `auth.users.raw_app_meta_data.role`. Anything else is
-- treated as a regular user (default-deny on admin-only paths). Admins are
-- managed by Supabase service_role (which is what the FastAPI backend uses);
-- the frontend reads the role off `user.app_metadata.role` after login.
--
-- This migration:
--   1. Promotes reinier@bustelberg.nl to admin if the user already exists.
--   2. Installs a BEFORE-INSERT trigger that auto-promotes the same email
--      on first signup (so prod won't be left without an admin if the user
--      hasn't signed up yet at deploy time).

UPDATE auth.users
SET raw_app_meta_data = COALESCE(raw_app_meta_data, '{}'::jsonb) || jsonb_build_object('role', 'admin')
WHERE email = 'reinier@bustelberg.nl';

CREATE OR REPLACE FUNCTION public.set_admin_role_on_signup()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
  IF NEW.email = 'reinier@bustelberg.nl' THEN
    NEW.raw_app_meta_data := COALESCE(NEW.raw_app_meta_data, '{}'::jsonb)
                          || jsonb_build_object('role', 'admin');
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS bbterminal_set_admin_role ON auth.users;
CREATE TRIGGER bbterminal_set_admin_role
  BEFORE INSERT ON auth.users
  FOR EACH ROW
  EXECUTE FUNCTION public.set_admin_role_on_signup();

NOTIFY pgrst, 'reload schema';
