-- `company.out_of_scope_at` + `company.out_of_scope_reason` —
-- set when an entry in `gf_ticker_overrides.json` is flagged
-- `{"unavailable": true, "reason": "..."}` for the company's
-- (iShares ticker, iShares exchange) pair.
--
-- Semantically distinct from `delisted_at` and
-- `gurufocus_lookup_failed_at`:
--
--   * delisted_at                = GuruFocus said "delisted" (paywall;
--                                  listing was real but no longer
--                                  tradable on the GuruFocus plan).
--   * gurufocus_lookup_failed_at = (ticker, exchange) doesn't resolve
--                                  on GuruFocus at all — primary plus
--                                  every fallback returned "Stock not
--                                  found". UI shows a red GF LOOKUP
--                                  badge inviting the user to probe.
--   * out_of_scope_at            = listing exists and is known to be on
--                                  a real exchange we deliberately
--                                  don't cover (e.g. Hamburg). The
--                                  override author tagged it; the
--                                  company stays in `company` for
--                                  visibility but is excluded from
--                                  universe_membership and skipped by
--                                  the price phase.
--
-- The /companies UI renders non-null rows with an amber OUT OF SCOPE
-- badge whose tooltip shows the reason string.

ALTER TABLE company
  ADD COLUMN IF NOT EXISTS out_of_scope_at TIMESTAMPTZ NULL;

ALTER TABLE company
  ADD COLUMN IF NOT EXISTS out_of_scope_reason TEXT NULL;

-- Partial index — only the rows we'd ever want to surface are the
-- non-null ones (a small slice of the table). Cuts index size vs. a
-- full-column index.
CREATE INDEX IF NOT EXISTS company_out_of_scope_at_idx
  ON company (out_of_scope_at)
  WHERE out_of_scope_at IS NOT NULL;

NOTIFY pgrst, 'reload schema';
