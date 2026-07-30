-- A MANUAL alias: one ISIN takes another's instrument, wholesale.
--
-- `isin` is served by `canonical_isin`'s analysis row — the same yfinance symbol, the same price
-- series, the same GuruFocus listing. Set by hand and re-applied after every resolution, so a
-- re-resolve cannot quietly put the aliased ISIN back on a listing of its own.
--
-- WHY IT EXISTS: the two identifiers for one business. `US8740391003` is TSMC's NYSE ADR and
-- `TW0002330008` is the Taiwan ordinary — two ISINs, two listings, one company. `company_override`
-- already does this in the company world (New Oriental NYSE:EDU vs its HK line, BP NYSE vs London);
-- this is the same act in the ASSET world, where the pipeline resolves per ISIN and would otherwise
-- give each its own instrument.
--
-- ⚠ THE TWO LISTINGS ARE NOT PRICED THE SAME, AND THE ALIAS DOES NOT PRETEND OTHERWISE. TSMC is
-- 1 ADR = 5 ordinary shares, and the ADR carries a premium besides (measured 2026-07-23: TSM
-- USD 421.21 against 2330.TW TWD 2,400 ≈ USD 73.2 × 5 = USD 366, a ~15% premium). An alias makes
-- both ISINs report the CANONICAL listing's price, so any holding booked in the other listing's
-- terms is valued in the canonical's. That is a deliberate choice about which series to trust, not
-- a currency conversion — the price check (`_airs_holding_isin`) will flag the difference as a
-- `price_mismatch`, and on an aliased row that flag is expected rather than a fault.
--
-- ⚠ NEVER ALIAS TWO DIFFERENT COMPANIES. The guard is the operator, not the schema: nothing here
-- can tell a share class from an unrelated instrument. Alias only where the two ISINs are the same
-- issuer, and record why in `note`.
CREATE TABLE IF NOT EXISTS public.asset_isin_alias (
    isin           text PRIMARY KEY,
    canonical_isin text NOT NULL,
    note           text,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT asset_isin_alias_not_self CHECK (isin <> canonical_isin)
);

CREATE INDEX IF NOT EXISTS idx_asset_isin_alias_canonical
    ON public.asset_isin_alias (canonical_isin);

REVOKE ALL ON public.asset_isin_alias FROM anon, authenticated;
GRANT ALL ON public.asset_isin_alias TO service_role;

-- TSMC: the NYSE ADR takes the Taiwan ordinary's instrument.
INSERT INTO public.asset_isin_alias (isin, canonical_isin, note)
VALUES ('US8740391003', 'TW0002330008',
        'TSMC: the NYSE ADR (US8740391003) is served by the Taiwan ordinary (TW0002330008). One company, two identifiers.')
ON CONFLICT (isin) DO NOTHING;

NOTIFY pgrst, 'reload schema';
