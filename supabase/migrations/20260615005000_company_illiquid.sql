-- Illiquid marker for companies whose listing trades infrequently, so
-- GuruFocus serves stale / sparse prices for it (e.g. Telecom Italia savings
-- shares MIL:TITR). Distinct from `delisted_at` (stopped trading entirely) and
-- `out_of_scope_at` (outside the GuruFocus subscription): an illiquid name still
-- trades occasionally, so it's still priced — but its perpetually-behind latest
-- close isn't a valid measure of how fresh our ACTIVE prices are, so the
-- price-coverage freshness check (/api/data/price-coverage) excludes it.
-- Set manually (the /schedule "mark illiquid" action); never auto-set.
ALTER TABLE company ADD COLUMN IF NOT EXISTS illiquid_at timestamptz;

COMMENT ON COLUMN company.illiquid_at IS
  'Set when a listing trades infrequently / GuruFocus serves stale prices for it. Excluded from the price-coverage freshness measure; still priced (it occasionally trades). Manual marker.';
