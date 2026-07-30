-- The identity of an AIRS account holding, decided by hand.
--
-- The normal route to a holding's ISIN is its model portfolio: the book carries a fund NAME and no
-- ISIN, the Fixed model carries the ISINs, and `_airs_holding_isin` pairs the two. That route dies
-- when the stored model snapshot has no position for the holding at all — measured 2026-07-23,
-- AIRS's Fixed portfolios hold `Invesco Wld EW ETF Acc` (IE000OEF25S1) while our newest available
-- snapshot (positions_datum 2025-04-28) still holds `Ish DJS GSD 100`. No amount of matching can
-- find an ISIN that is not in the data, so a human supplies it.
--
-- ⚠ KEYED ON THE HOLDING NAME, NOT ON (PORTFOLIO, HOLDING). What instrument a fund name denotes is
-- a property of the name, not of which book happens to hold it — the Invesco line above appears in
-- FOUR books and is one fact, so it is stored once. Keyed per book it would be entered four times
-- and the copies would be free to disagree. (Same rule as the model-portfolio Link.)
--
-- ⚠ IT DOES NOT SKIP THE PRICE CHECK. An override says which instrument this is; it does not say
-- the book agrees. The implied price is still compared against that ISIN's own close, so a typo'd
-- or wrong ISIN comes back `price_mismatch` rather than being trusted because a human typed it.
CREATE TABLE IF NOT EXISTS public.airs_holding_isin_override (
    holding_name text PRIMARY KEY,
    isin         text NOT NULL,
    note         text,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- AIRS's own spelling is what gets stored, but two rows differing only in case or padding would be
-- two conflicting answers to one question. Matching is case-insensitive, so uniqueness must be too.
CREATE UNIQUE INDEX IF NOT EXISTS airs_holding_isin_override_name_key
    ON public.airs_holding_isin_override (lower(btrim(holding_name)));

REVOKE ALL ON public.airs_holding_isin_override FROM anon, authenticated;
GRANT ALL ON public.airs_holding_isin_override TO service_role;

-- The measured case above. In our grid as MWEP.L (London, GBp, 454 bars, ~EUR 992k/day), so the
-- price check has something to verify it against the moment the override lands.
INSERT INTO public.airs_holding_isin_override (holding_name, isin, note)
VALUES ('Invesco World Equal Weight ETF Acc', 'IE000OEF25S1',
        'AIRS Fixed lists it as "Invesco Wld EW ETF Acc"; our newest model snapshot (2025-04-28) predates the swap and still carries Ish DJS GSD 100.')
ON CONFLICT (holding_name) DO NOTHING;

NOTIFY pgrst, 'reload schema';
