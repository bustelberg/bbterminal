-- `company.orphaned_at` — set when a company belongs to NO source universe
-- (live LongEquity/ACWI/Leonteq), is NOT out-of-scope, and is NOT a member of
-- any frozen snapshot. Previously such rows were DELETED by the pipeline's
-- prune phase; the user wants them KEPT + visibly flagged in /companies
-- instead. The prune phase now sets/clears this timestamp (`mark_orphan_companies`)
-- rather than deleting, so an orphan is retained and badged "NO UNIVERSE", and
-- the flag clears automatically if the company later re-joins a universe.

ALTER TABLE company
    ADD COLUMN IF NOT EXISTS orphaned_at timestamptz;
