-- AIRS's OWN ISIN per account holding.
--
-- The Vermogensoverzicht (VOLK) export gained an `ISIN-code` column on 2026-07-23. Until then the
-- book carried only `Fondsomschrijving` — a NAME — which is the entire reason
-- `routers/_airs_holding_isin.py` exists: it recovers each holding's identity by fuzzy-matching
-- that name against the Fixed portfolio's positions, assigning 1:1, and price-checking the result.
--
-- That machinery is sound but it is inference, and inference has a floor it cannot get under: when
-- the stored model snapshot predates a swap in AIRS, a holding has NO position to pair with, and a
-- 1:1 assignment cannot answer "none". Measured 2026-07-23, four books reported `Invesco Wld EW
-- ETF Acc` as DE000A0F5UH1 (`Ish DJS GSD 100`) for exactly that reason.
--
-- With this column the join is EXACT and none of that applies. The name route stays as the
-- fallback and must: every snapshot taken before today has no ISIN, the cash line never will, and
-- a portfolio whose export omits the column still has to resolve.
--
-- ⚠ NULLABLE, AND NULL IS NOT "NO ISIN" — it is "this snapshot did not carry one". A NOT NULL here
-- would have to invent a value for every historical row.
ALTER TABLE airs_holding
    ADD COLUMN IF NOT EXISTS isin text;

-- The join `_airs_holding_isin` now does first: this account's holdings, by ISIN.
CREATE INDEX IF NOT EXISTS idx_airs_holding_isin ON airs_holding (isin) WHERE isin IS NOT NULL;

NOTIFY pgrst, 'reload schema';
