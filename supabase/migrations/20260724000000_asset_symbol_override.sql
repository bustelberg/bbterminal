-- A manual, durable answer to "which Yahoo listing IS this ISIN?".
--
-- WHY THIS EXISTS
--   `asset_execution.yahoo_symbol` is written by every resolver we have — `fast_resolve`, the
--   queue worker, both repointers and the per-row Resolve action. A correction made by editing
--   that column is therefore not a decision, it is a value, and the next thing that writes the
--   row silently discards it. Every other manual fix in this codebase already learned this:
--   `company_override` is re-applied on every ingest, `asset_isin_alias` after every resolution,
--   and `gf_ticker_overrides.json` exists because the SP500 and Leonteq paths both rewrite
--   `gurufocus_ticker`. This is the same fact for the yfinance symbol.
--
-- ⚠ IT IS NOT `asset_isin_alias`, AND THE DIFFERENCE IS THE WHOLE POINT.
--   An alias says "ISIN A is deliberately served by ISIN B's instrument" — an ADR priced off its
--   ordinary, two securities sharing ONE series on purpose. This table says the opposite: this
--   ISIN has its OWN listing and the automatic path picked the wrong one. Recording a wrong-listing
--   fix as an alias would claim a relationship between two securities that does not exist.
--
-- THE CASE THAT PROVED IT (2026-07-24)
--   IE00BJSFQW37 — iShares Global Corp Bond UCITS ETF, EUR-hedged Dist — was resolved onto
--   `IS0X.DE`, which is the USD UNHEDGED Dist share class. Different ISIN, different currency
--   exposure, different compounding; the names differ only by "EUR" vs "USD (Dist)", so the
--   name-anchored resolver accepted it. OpenFIGI lists 36 venues for that ISIN and `IS0X` is not
--   among any of them; the correct German line is `36B7.DE`. Measured: AIRS implied EUR 4.1523
--   per unit, `IS0X.DE` closed at EUR 77.55, `36B7.DE` at EUR 4.1523 — an exact match. The wrong
--   listing was held in 5 model portfolios at weights up to 30%.
--
-- ⚠ AND IT WAS THE *MORE* LIQUID ONE. `IS0X.DE` does EUR 222k/day against `36B7.DE`'s EUR 110k.
--   Every automatic repointer ranks by liquidity, so none of them would ever choose the correct
--   listing here, and `repoint_etf_listing` correctly refused to judge it at all ("IS0X.DE is not
--   among this ISIN's listings, so the candidate set is incomplete"). A human has to name it —
--   which is exactly why the answer needs somewhere to live.
CREATE TABLE IF NOT EXISTS public.asset_symbol_override (
    isin         text PRIMARY KEY,
    -- The Yahoo symbol this ISIN must resolve to. Verified to have a real price series before
    -- being stored: a symbol with no bars is not a listing (the GODE.DE incident, where ten
    -- structured products were written onto one empty series with status='ok').
    yahoo_symbol text NOT NULL,
    -- WHY. A bare mapping is unreviewable a year later — it cannot be told apart from a typo.
    note         text,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- ⚠ An ISIN and its override target must not be the same claim twice under different spellings.
CREATE UNIQUE INDEX IF NOT EXISTS asset_symbol_override_isin_key
    ON public.asset_symbol_override (upper(btrim(isin)));

ALTER TABLE public.asset_symbol_override ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS asset_symbol_override_deny_all ON public.asset_symbol_override;
CREATE POLICY asset_symbol_override_deny_all ON public.asset_symbol_override FOR ALL USING (false);

REVOKE ALL ON public.asset_symbol_override FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.asset_symbol_override TO service_role;

-- ⚠ THE ROWS SHIP WITH THE TABLE, OR THE FIX IS LOCAL-ONLY. A repoint corrects
-- `asset_execution` and `asset_price`, both of which are DATA — they do not travel with a git
-- merge, so an empty table on prod means prod keeps the wrong listing while the code that would
-- fix it sits there unused. Seeding here is the same thing `asset_isin_alias` does for the TSMC
-- alias, and for the same reason. Applying them is still a deliberate step:
--     uv run python scripts/apply_symbol_overrides.py

-- iShares Global Corp Bond UCITS ETF, EUR-hedged Dist. Resolved by name onto IS0X.DE — the USD
-- UNHEDGED Dist class of the same fund, a different ISIN at EUR 77.55 against this one's EUR 4.15.
-- OpenFIGI lists 36 venues for this ISIN and IS0X is not among them. Verified against AIRS:
-- implied EUR 4.1523/unit == 36B7.DE close 4.1523 on 2026-07-22. Held in 5 models, up to 30%.
-- ⚠ The wrong listing was the MORE liquid one (EUR 222k/day vs 110k), so no ranker can fix this.
INSERT INTO public.asset_symbol_override (isin, yahoo_symbol, note)
VALUES ('IE00BJSFQW37', '36B7.DE',
        'EUR-hedged Dist share class. The resolver picked IS0X.DE by name, which is the USD UNHEDGED Dist class of the same fund (different ISIN, EUR 77.55 vs EUR 4.15). OpenFIGI lists 36 venues for this ISIN and IS0X is not among them. Verified against AIRS: implied EUR 4.1523/unit == 36B7.DE close 4.1523 on 2026-07-22.')
ON CONFLICT (isin) DO NOTHING;

-- Samsung Electronics ordinary. Resolved onto SMSN.IL — the LONDON GDR, a different instrument
-- with its own ISIN, quoted USD ~4,322 against the ordinary's KRW 249,500 (~25x per unit).
-- OpenFIGI lists 49 venues for this ISIN and SMSN is not among them.
-- ⚠ SMSN.IL is NOT thin (EUR 108m/day) and its ADV-to-market-cap ratio is healthy, which is why
-- `repoint_primary_listing` never flagged it. Only the ISIN reveals it is another security.
INSERT INTO public.asset_symbol_override (isin, yahoo_symbol, note)
VALUES ('KR7005930003', '005930.KS',
        'Korean ordinary share. The resolver picked SMSN.IL — the LONDON GDR, a different instrument with its own ISIN, quoted USD ~4,322 against the ordinary''s KRW 249,500 (~25x per unit). OpenFIGI lists 49 venues for this ISIN and SMSN is not among them. AIRS holds the ordinary: implied EUR 154.25/unit.')
ON CONFLICT (isin) DO NOTHING;

-- 3i Group, the London ordinary (quoted in PENCE). Was on IGQ5.SG — Stuttgart, EUR — and this one
-- is a different failure from the two above: the SYMBOL is a real listing of this ISIN, but Yahoo's
-- CHART endpoint for it returns the LONDON PENCE series while its QUOTE endpoint says EUR 32.31.
-- So pence were stored under a EUR-labelled row, `_rate` saw "EUR" and applied no ÷100 divisor,
-- and the holding priced at EUR 2,552 against AIRS's EUR 31.34 — an 81x error.
-- ⚠ AND IT POISONS THE RANKER. `med_adv_eur` is computed from that same chart, so IGQ5.SG scored
-- EUR 6.47bn/day against III.L's real 79.7m — 85x, which is exactly the pence-as-EUR scale factor.
-- Every liquidity-ranking resolver will therefore PREFER the broken line. No ranker can fix this;
-- only the unit can. After repointing: ratio 1.0003, verdict ok.
INSERT INTO public.asset_symbol_override (isin, yahoo_symbol, note)
VALUES ('GB00B1YW4409', 'III.L',
        'London ordinary, quoted GBp. Was on IGQ5.SG (Stuttgart, EUR) whose Yahoo CHART returns the London PENCE series while its quote endpoint says EUR 32.31 — so pence were stored under a EUR-labelled row and _rate applied no divisor (81x error). It also inflated med_adv_eur to EUR 6.47bn/day against III.L real 79.7m, so every liquidity ranker preferred it.')
ON CONFLICT (isin) DO NOTHING;

NOTIFY pgrst, 'reload schema';
