-- Record who made each active company-sector management decision.  The id is deliberately not
-- an FK to auth.users: deleting a login must not erase or block the attribution on an editorial
-- decision.  Email is the denormalized, human-readable identity at the time of the last change.
ALTER TABLE public.company_sector_override
  ADD COLUMN IF NOT EXISTS updated_by_user_id uuid,
  ADD COLUMN IF NOT EXISTS updated_by_email text;

COMMENT ON COLUMN public.company_sector_override.updated_by_user_id IS
  'Verified auth user id that last set this active override.';
COMMENT ON COLUMN public.company_sector_override.updated_by_email IS
  'Verified auth email at the time this active override was last set.';

NOTIFY pgrst, 'reload schema';
