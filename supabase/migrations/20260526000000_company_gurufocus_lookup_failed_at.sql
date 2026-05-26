-- `company.gurufocus_lookup_failed_at` — last timestamp at which the
-- GuruFocus price/volume ingest got "Stock not found" for this company's
-- (gurufocus_ticker, exchange) pair AFTER trying every entry in the
-- exchange fallback list. NULL = lookups working (or never attempted).
--
-- Distinct from `delisted_at`:
--   * delisted_at      = real listing existed once, GuruFocus now reports
--                        "Delisted stocks are available for Professional
--                        plan" (a paywall, but the listing was real).
--   * gurufocus_lookup_failed_at = (ticker, exchange) doesn't resolve at
--                        all on GuruFocus. Usually the row's exchange is
--                        wrong (e.g. NYSE:ASND when the listing is actually
--                        NASDAQ:ASND), or the ticker symbol is stale.
--
-- The /companies UI surfaces non-null rows with a red badge so the user
-- can investigate before the next backtest fires the same "N companies
-- have NO price data" warning.

ALTER TABLE company
  ADD COLUMN IF NOT EXISTS gurufocus_lookup_failed_at TIMESTAMPTZ NULL;

-- Partial index — only the rows we'd ever want to surface are the
-- non-null ones (a tiny fraction of the table). Cuts index size vs. a
-- full-column index.
CREATE INDEX IF NOT EXISTS company_gurufocus_lookup_failed_at_idx
  ON company (gurufocus_lookup_failed_at)
  WHERE gurufocus_lookup_failed_at IS NOT NULL;

NOTIFY pgrst, 'reload schema';
