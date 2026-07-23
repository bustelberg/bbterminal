-- The AIRS Mutaties journal (`rapport_types=MUT`): the income a price return cannot see.
--
-- Every return on /portfolios is `Huidige waarde / Beginwaarde - 1`, which by construction cannot
-- see a dividend: the money leaves the position's value and arrives as cash. This is where it goes.
--
-- ⚠ THE RAW LINES ARE STORED, NEVER A PER-HOLDING TOTAL. A stored sum is a second source of truth
-- that drifts from the rows it counts (same rule as "the count is a VIEW, never a column"), and it
-- could not answer "which payments, on what dates" — which is exactly what a reader asks when a
-- dividend figure looks wrong. The aggregation is `airs_mutaties.direct_result`, on read.
--
-- ⚠ `amount_eur` IS AIRS'S OWN SIGNED EUR FIGURE. Dividend rows are positive, `Dividendbelasting`
-- rows NEGATIVE. It is stored exactly as reported: re-deriving it from Debet/Credit, or applying
-- `Valutakoers` a second time, is how you double-count or flip a sign.
--
-- ⚠ THERE IS NO ISIN ON THIS SHEET. `fonds` is a NAME, joined to `airs_holding.holding_name`
-- EXACTLY (both are AIRS strings truncated at the same 50 chars). Nothing fuzzy belongs here.
CREATE TABLE IF NOT EXISTS airs_mutatie (
    id            bigserial PRIMARY KEY,
    portefeuille  text NOT NULL,
    boekdatum     date,
    grootboek     text NOT NULL,        -- 'Dividend' | 'Dividendbelasting' | (anything new)
    fonds         text NOT NULL,        -- the instrument, BY NAME
    omschrijving  text,
    amount_eur    numeric NOT NULL,     -- AIRS's own signed EUR amount
    amount_local  numeric,              -- 'Bedrag vv', the payment in its own currency
    currency      text,
    fx_rate       numeric,              -- 'Valutakoers', the rate AIRS applied. Informational.
    retrieved_at  timestamptz NOT NULL DEFAULT now()
);

-- The read: one account's journal, and the per-fund roll-up over it.
CREATE INDEX IF NOT EXISTS idx_airs_mutatie_portfolio ON airs_mutatie (portefeuille, boekdatum);
CREATE INDEX IF NOT EXISTS idx_airs_mutatie_fonds ON airs_mutatie (portefeuille, fonds);

ALTER TABLE airs_mutatie ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS airs_mutatie_deny_all ON airs_mutatie;
CREATE POLICY airs_mutatie_deny_all ON airs_mutatie FOR ALL USING (false);

NOTIFY pgrst, 'reload schema';
