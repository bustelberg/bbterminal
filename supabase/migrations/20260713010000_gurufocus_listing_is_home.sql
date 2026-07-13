-- Is the resolved listing the asset row's OWN listing, or a fallback?
--
-- `isin/{ISIN}` returns every listing of the security worldwide, and they do NOT
-- carry equally good data. Measured 2026-07-13, Apple's dividend feed by listing:
--
--     AAPL (Nasdaq)  91 payments      <- home
--     XTER:APC       82
--     XSWX:AAPL      63   ... and with a FIVE-YEAR HOLE (2026-02 -> 2021-02)
--     MIL:1AAPL      35
--
-- The amounts agree exactly (0.27 USD on every line, ratio 1.0000 — GuruFocus
-- reports a dividend in its DECLARATION currency regardless of venue, so a foreign
-- listing is NOT a wrong-currency hazard, which is what we first assumed). The
-- difference is COMPLETENESS, and it is invisible: a holed series still sums to a
-- confident-looking "trailing 12m".
--
-- So we take the best listing available and record whether it was the row's own
-- (`is_home` — matched both its ticker and its currency). A fallback listing is
-- charted, because its data is real; it is just labelled as possibly-partial, and
-- `_trailing_12m` refuses to sum a window spanning a gap.

ALTER TABLE public.gurufocus_listing
    ADD COLUMN IF NOT EXISTS is_home boolean NOT NULL DEFAULT false;

-- The DEFAULT above backfills `false` onto every row resolved BEFORE this column
-- existed — silently branding a correct home listing "not home". That is not
-- hypothetical: it hit NASDAQ:QQQ, a perfect ticker+currency match, which then showed
-- the amber "history may be incomplete" warning it had done nothing to deserve. A
-- default is not a computed value, and here there is no default that could be right:
-- `true` would suppress a warning that is sometimes warranted, `false` raises one that
-- is often not.
--
-- This table is only a CACHE of one API call per ISIN, so the honest repair is to drop
-- the un-computed rows and let them re-resolve on demand. Costs one GuruFocus call per
-- ISIN, once. (No-op on prod, where the table is new and empty.)
DELETE FROM public.gurufocus_listing;

NOTIFY pgrst, 'reload schema';
