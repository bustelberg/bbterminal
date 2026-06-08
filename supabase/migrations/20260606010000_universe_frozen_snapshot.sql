-- Static (frozen) universe snapshots.
--
-- A frozen universe is a one-time copy of a template-managed universe's
-- monthly memberships into a NON-template universe row (`template_key IS
-- NULL`), so a backtest can pin a reproducible universe that the scheduled
-- pipeline never re-reconstructs. `_load_index_universe` already falls back
-- from `template_key == label` to `label == label`, so a frozen universe
-- loads through the identical path with no loader change.
--
-- `frozen_at` doubles as the marker (non-NULL = a static snapshot) and the
-- cutoff timestamp; `frozen_from` records which template it was copied from.
ALTER TABLE public.universe
  ADD COLUMN IF NOT EXISTS frozen_at  timestamptz,
  ADD COLUMN IF NOT EXISTS frozen_from text;

COMMENT ON COLUMN public.universe.frozen_at IS
  'When this universe was frozen as a static snapshot. NULL = live template / user-criteria universe. Non-NULL marks a static snapshot the pipeline never refreshes (and that prune protects).';
COMMENT ON COLUMN public.universe.frozen_from IS
  'template_key of the template this snapshot was copied from (e.g. LEONTEQ). NULL for non-frozen universes.';
