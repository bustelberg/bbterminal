-- A DYNAMIC portfolio's own model weights, from `rapport_types=MODEL`.
--
-- ⚠ THIS RETIRES THE FIXED↔DYNAMIC PAIRING. A book's strategy weights used to live in a separate
-- AirSPMS portfolio (`*_FX`/`*_AFS`) that had to be PAIRED to the book — a name guess on 27 of 28
-- accounts, where a mis-pairing files a real book's money under another strategy's name and
-- nothing else on the row looks wrong (the risk variants of a strategy hold the same instruments).
-- The MODEL report is scoped to ONE dynamic portfolio and states its weights directly, so there is
-- nothing left to pair.
--
-- ⚠ KEYED BY (portefeuille, fonds) — a NAME, and that is safe HERE in a way the old join was not:
-- both strings come from the same portfolio in the same system, and 40 of 42 match byte-for-byte.
-- The one systematic difference is the cash line (`Effectenrekening Liquiditeiten` here vs
-- `Effectenrekening` in the Vermogensoverzicht), aliased explicitly in `airs_model.NAME_ALIASES`.
--
-- ⚠ A ROW HERE WITH NO MATCHING HOLDING IS DRIFT, NOT AN ERROR: the strategy says hold something
-- the book has not bought. Measured, `iShares Global Select Dividend 100` on BUS_Neutraal_Dyn.
-- Percentages are PERCENTS (3.25), not fractions, and `model_pct` sums to exactly 100 per book.
CREATE TABLE IF NOT EXISTS airs_model_weight (
    portefeuille    text NOT NULL,
    fonds           text NOT NULL,          -- already aliased to the holdings' spelling
    model_pct       numeric,                -- Model percentage
    actual_pct      numeric,                -- Werkelijk percentage
    drift_pct       numeric,                -- Afwijking percentage
    drift_eur       numeric,                -- Afwijking in euro
    buy             numeric,                -- Kopen
    sell            numeric,                -- Verkopen
    model_value_eur numeric,                -- Waarde volgens model
    retrieved_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (portefeuille, fonds)
);

CREATE INDEX IF NOT EXISTS idx_airs_model_weight_portfolio ON airs_model_weight (portefeuille);

ALTER TABLE airs_model_weight ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS airs_model_weight_deny_all ON airs_model_weight;
CREATE POLICY airs_model_weight_deny_all ON airs_model_weight FOR ALL USING (false);

NOTIFY pgrst, 'reload schema';
