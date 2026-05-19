-- Track when each universe was last refreshed. Powers cache-invalidation:
-- the in-process membership / full-universe LRU caches compare against
-- this timestamp; HTTP responses include an ETag derived from it so
-- browsers can short-circuit repeat queries with a 304.
--
-- Nullable so pre-existing rows (e.g. SP500 still on the old static
-- model) don't have to be retroactively dated. New rows + every
-- successful `UniverseTemplate.refresh()` write a current timestamp.

ALTER TABLE universe
  ADD COLUMN IF NOT EXISTS last_refreshed_at TIMESTAMPTZ;

COMMENT ON COLUMN universe.last_refreshed_at IS
  'Set by UniverseTemplate.refresh() on every successful write. Used as '
  'the cache-invalidation key + HTTP ETag input. NULL on universes that '
  'predate the template abstraction.';

NOTIFY pgrst, 'reload schema';
