-- Raw Leonteq underlying-equity scrape data.
--
-- Captures what Leonteq publishes on their structured-products underlyings
-- listing page (https://structuredproducts-ch.leonteq.com/services/underlyings):
-- equity name + Leonteq's own sector + industry classification + identifier
-- fields, plus a derived GuruFocus link and an optional FK to the canonical
-- `company` row when we can resolve one.
--
-- Stored separately from `universe_membership` because the membership table
-- doesn't have a column for `industry` — and Leonteq's industry breakdown
-- (sector → industry → companies) is the whole point of the /leonteq page.
-- The `LeonteqTemplate.refresh()` flow ALSO writes a row per scraped equity
-- into `universe_membership` (keyed by template_key='LEONTEQ') so the
-- universe-template machinery + /schedule integration still works — that
-- row only carries sector, not industry.

CREATE SEQUENCE IF NOT EXISTS leonteq_equity_id_seq;

CREATE TABLE IF NOT EXISTS leonteq_equity (
  id              INTEGER PRIMARY KEY DEFAULT nextval('leonteq_equity_id_seq'),
  name            TEXT NOT NULL,
  ticker          TEXT,
  isin            TEXT,
  sector          TEXT,
  industry        TEXT,
  -- Built from ticker+exchange once we resolve to a company row.
  gurufocus_url   TEXT,
  -- FK to the canonical company. Null when reconciliation can't
  -- match (new name we haven't seen, or no company row yet).
  company_id      INTEGER REFERENCES company(company_id) ON DELETE SET NULL,
  -- The scrape that produced this row. Lets us delete-and-replace on
  -- each refresh by `scraped_at`, and surfaces "data is N days stale"
  -- on the /leonteq page.
  scraped_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_leonteq_equity_scraped_at
  ON leonteq_equity (scraped_at DESC);

CREATE INDEX IF NOT EXISTS idx_leonteq_equity_sector
  ON leonteq_equity (sector, industry);

CREATE INDEX IF NOT EXISTS idx_leonteq_equity_company_id
  ON leonteq_equity (company_id);

NOTIFY pgrst, 'reload schema';
