-- Replace airs_holding's composite PK (portefeuille, as_of_date, holding_name)
-- with a surrogate `id`. The composite was too strict: a portfolio can hold the
-- SAME fund on two lines (e.g. two tranches), e.g. "6,5% Rabobank Certificaten
-- 14-perp." twice in BUS_Neutraal_Dyn → the daily refresh's insert hit a
-- duplicate-key violation. Per-day dedup stays correct via delete-then-insert
-- per (portefeuille, as_of_date) in the refresh job.
--
-- Idempotent: works whether the table still has the composite PK (prod) or was
-- already recreated with the surrogate id (a dev that picked up the original
-- migration's later edit).

-- Drop whatever PRIMARY KEY exists (composite or surrogate — both are named
-- airs_holding_pkey).
ALTER TABLE airs_holding DROP CONSTRAINT IF EXISTS airs_holding_pkey;

-- Add the surrogate id if missing (backfills existing rows with 1,2,3,…).
ALTER TABLE airs_holding ADD COLUMN IF NOT EXISTS id bigint GENERATED ALWAYS AS IDENTITY;

-- Make id the PK if the table currently has none.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'airs_holding'::regclass AND contype = 'p'
  ) THEN
    ALTER TABLE airs_holding ADD PRIMARY KEY (id);
  END IF;
END $$;

NOTIFY pgrst, 'reload schema';
