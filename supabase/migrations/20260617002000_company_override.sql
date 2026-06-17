-- Manual company-resolution overrides — the prevention layer for dupes that
-- the ISIN + name dedup passes can't catch, re-applied on every ingest.
--
-- Two kinds:
--   'alias'   — a company under `isin` (a secondary listing, e.g. a home-market
--               H-share) is the SAME issuer as the company under `canonical_isin`
--               (e.g. its US ADR). The override pass merges the secondary INTO
--               the canonical (memberships move, the secondary's prices are
--               dropped — different listing/currency). Catches the cross-ISIN
--               dupes ISIN-dedup misses (different ISIN) and name-dedup misses
--               (both have ISINs / names differ): New Oriental ADR vs HK,
--               BP ADR vs London. Auto-recorded when you consolidate cross-ISIN.
--   'exclude' — a real but UNWANTED constituent (e.g. GE Vernova T&D India, an
--               NSE listing with no ISIN). Matched by `isin`, else by
--               (`ticker`,`exchange`). The pass marks it `out_of_scope_at` so it
--               stays suppressed even after an index reconstruction re-creates it.

CREATE TABLE IF NOT EXISTS company_override (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind            text NOT NULL CHECK (kind IN ('alias', 'exclude')),
    isin            text,           -- secondary ISIN (alias) / listing ISIN (exclude)
    ticker          text,           -- alt match key when the listing has no ISIN
    exchange        text,           -- alt match key (gurufocus exchange code)
    canonical_isin  text,           -- alias target: the company to KEEP
    note            text,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- One alias per secondary ISIN; lets the merge auto-record idempotently.
CREATE UNIQUE INDEX IF NOT EXISTS company_override_alias_isin_uniq
    ON company_override (isin) WHERE kind = 'alias';
