-- ISIN -> GuruFocus listing, cached.
--
-- WHY THIS EXISTS
--   The /asset-pipeline Div/share column bridges an asset row to GuruFocus via
--   `company` (ISIN -> company.isin -> gurufocus_ticker). ETFs are never ingested
--   into `company`, so ~87% of the grid — every ETF, crypto and commodity row —
--   can never reach a dividend, even though GuruFocus HAS the data (probed:
--   QQQ returns 89 per-share distributions).
--
--   GuruFocus's undocumented `isin/{ISIN}` endpoint resolves an ISIN straight to
--   [{symbol, exchange}] with no company row involved. This table caches that
--   resolution so we pay ONE API call per ISIN, ever.
--
-- WHY `status` IS NOT JUST "did we find it"
--   `isin/{ISIN}` returns EVERY listing worldwide — Apple comes back with 19
--   (Vienna, Sofia, Zurich, Frankfurt, Stuttgart, Milan, Kazakhstan, Mexico, …).
--   Picking the wrong one is the NVDA-on-Stuttgart trap with a new hat: Apple's
--   XTER:APC line reports its dividend in EUR, which is a different number from
--   the USD one. So the resolver REFUSES to guess when the candidates don't
--   single one out, and records why:
--     ok           — one listing matched this row on currency and/or ticker
--     not_found    — GuruFocus does not know this ISIN
--     unsubscribed — listings exist, but none on an exchange we subscribe to
--     no_match     — listings exist, but none matches THIS row's ticker or currency.
--                    Real case: iShares Core MSCI World (IE00B4L5Y983) is EUNL.DE
--                    (Xetra, EUR) in our grid, and GuruFocus's only listing for that
--                    ISIN is OTCPK:IRRRF (US OTC, USD). One candidate, and still the
--                    wrong one — which is why a lone candidate is NOT auto-accepted.
--     ambiguous    — several equally-good candidates; picking one would be a coin flip
--   A row with status <> 'ok' is a NEGATIVE cache: it stops us re-spending an API
--   call on an ISIN we already know we cannot resolve.
--
-- `candidates` keeps the raw GuruFocus response so a later, smarter picker can be
-- re-run offline against what the API actually said — no re-fetch, no lost evidence.

CREATE TABLE IF NOT EXISTS public.gurufocus_listing (
    isin             text PRIMARY KEY,
    -- The chosen listing. NULL for every status other than 'ok'.
    gurufocus_ticker text,
    exchange_code    text,
    status           text NOT NULL DEFAULT 'ok',
    -- Raw [{symbol, exchange}] from GuruFocus, exactly as returned.
    candidates       jsonb,
    checked_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS gurufocus_listing_status_idx ON public.gurufocus_listing(status);

REVOKE ALL ON public.gurufocus_listing FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.gurufocus_listing TO service_role;

NOTIFY pgrst, 'reload schema';
