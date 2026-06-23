-- Stored AIRS "Vermogensoverzicht" (wealth/holdings overview) per portfolio per
-- working day. Today AIRS holdings only exist transiently (drag-drop parse, no
-- DB) — this persists the daily scheduled scrape so the /airs-portfolio page can
-- show each portfolio's positions + YTD without re-downloading.
--
-- One row per (portfolio, as_of_date, holding). Mirrors `portfolio.ParsedHolding`
-- (`parse_airs_excel`), the same parser the drag-drop path uses.

CREATE TABLE IF NOT EXISTS airs_holding (
    -- Surrogate PK: a portfolio can legitimately hold the SAME fund on two
    -- lines (e.g. two tranches/lots), so (portfolio, date, name) is NOT unique.
    -- Per-day dedup is by delete-then-insert per (portefeuille, as_of_date).
    id                    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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
    retrieved_at          timestamptz NOT NULL DEFAULT now()
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
