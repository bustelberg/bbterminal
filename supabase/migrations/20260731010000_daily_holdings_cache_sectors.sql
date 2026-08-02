-- Per-SECTOR scores for a cached "Daily holdings" day.
--
-- WHY IT IS A SECOND COLUMN AND NOT PACKED INTO `holdings`
--   `holdings` is a list of companies; this is a list of sectors, computed over the whole
--   selection POOL rather than over the picked names. Different grain, different length, and the
--   pool includes sectors that were NOT chosen -- which is the informative part. Folding two
--   shapes into one column means every reader has to know which half it is holding.
--
-- ⚠ WITHOUT IT A CACHED DAY RENDERS BLANK SECTOR SCORES WHILE A FRESHLY COMPUTED ONE DOES NOT.
--   That is the worst failure mode available here: the table is complete for the newest few days
--   (which are always recomputed) and empty for everything older, which reads as "we only started
--   scoring sectors recently" rather than "the cache predates this column".
--
-- ⚠⚠ THE DEFAULT DOES NOT MAKE THAT SELF-HEALING, AND AN EARLIER VERSION OF THIS COMMENT CLAIMED
--   IT DID. "Refreshed on the next recompute of that day" is false: the reader only ever recomputes
--   the newest few days, so a row written before this column existed is served with an empty list
--   for ever. Measured immediately after shipping it -- 58 of 150 cached days came back empty and
--   the sector-rank chart drew flat gaps across three months while the rest looked correct.
--   The fix is in the READER, not here: `_load_cached_selections` treats holdings-without-sectors
--   as stale and recomputes the day. A day that has holdings always has sectors (they come off the
--   same scored frame), so the empty list is unambiguous evidence of a legacy row.
ALTER TABLE public.daily_holdings_cache
    ADD COLUMN IF NOT EXISTS sector_scores jsonb NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN public.daily_holdings_cache.sector_scores IS
    'Per-sector momentum/price/volume score for this day, over the same pool the sector ranking '
    'was computed on. Every sector in the pool, not only the chosen ones.';

NOTIFY pgrst, 'reload schema';
