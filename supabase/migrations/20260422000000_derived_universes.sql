-- Derived universes: tighten a base universe by threshold-filtering quality metrics.
--
-- A universe with parent_universe_id IS NULL is a "base" universe (ACWI, LongEquity
-- SP500, etc.). A derived universe copies a subset of the parent's monthly
-- memberships, filtered by a per-company `filter_config` applied against
-- derived metric values stored in metric_data (source_code='derived').

ALTER TABLE universe
  ADD COLUMN IF NOT EXISTS parent_universe_id INTEGER
    REFERENCES universe(universe_id) ON DELETE SET NULL;

ALTER TABLE universe
  ADD COLUMN IF NOT EXISTS filter_config JSONB;

CREATE INDEX IF NOT EXISTS idx_universe_parent
  ON universe (parent_universe_id);
