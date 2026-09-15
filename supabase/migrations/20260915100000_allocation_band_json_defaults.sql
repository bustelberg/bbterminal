-- The versioned JSON file is the policy baseline.  This table now records only
-- an administrator's deliberate deviation from that baseline.
ALTER TABLE public.airs_allocation_band
  ADD COLUMN IF NOT EXISTS is_override boolean NOT NULL DEFAULT false;

-- All pre-existing rows came from the former database seed.  Retain them for
-- audit/history, but do not let that obsolete seed shadow the JSON defaults.
UPDATE public.airs_allocation_band
   SET is_override = false;

COMMENT ON COLUMN public.airs_allocation_band.is_override IS
  'True only for an administrator override of backend/config/allocation_bands.defaults.json.';

NOTIFY pgrst, 'reload schema';
