-- Stored AIRS "Vermogensoverzicht" (wealth/holdings overview) per portfolio per
-- working day. Today AIRS holdings only exist transiently (drag-drop parse, no
-- DB) — this persists the daily scheduled scrape so the /airs-portfolio page can
-- show each portfolio's positions + YTD without re-downloading.
--
-- One row per (portfolio, as_of_date, holding). Mirrors `portfolio.ParsedHolding`
-- (`parse_airs_excel`), the same parser the drag-drop path uses.

-- NOTE: this composite PK proved too strict (a portfolio can hold the same fund
-- on two lines) — it's replaced by a surrogate `id` PK in the follow-up
-- migration 20260623030000_airs_holding_surrogate_pk.sql. Kept as-is here to
-- match what was already applied to prod.
CREATE TABLE IF NOT EXISTS airs_holding (
    portefeuille          text NOT NULL,
    as_of_date            date NOT NULL,
    holding_name          text NOT NULL,
    quantity              numeric,
    currency              text,
    weight                numeric,            -- fraction of portfolio current EUR value
    start_value_eur       numeric,            -- Beginwaarde lopend jaar EUR
    current_value_eur     numeric,            -- Huidige waarde EUR
    ytd_return_eur        numeric,
    ytd_return_pct        numeric,            -- EUR basis
    ytd_return_local_pct  numeric,            -- currency-neutral (local)
    retrieved_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (portefeuille, as_of_date, holding_name)
);

-- Common reads: "latest snapshot for a portfolio" and "everything as of a date".
CREATE INDEX IF NOT EXISTS idx_airs_holding_portfolio_date
    ON airs_holding (portefeuille, as_of_date DESC);

-- RLS: deny-all (the backend uses the service key, which bypasses) — matches the
-- project default; AIRS portfolio data must not be readable via the anon key.
ALTER TABLE airs_holding ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS airs_holding_deny_all ON airs_holding;
CREATE POLICY airs_holding_deny_all ON airs_holding FOR ALL USING (false);

GRANT SELECT, INSERT, UPDATE, DELETE ON airs_holding TO service_role;

NOTIFY pgrst, 'reload schema';
