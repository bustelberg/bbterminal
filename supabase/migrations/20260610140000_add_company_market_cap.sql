-- Market-cap snapshot per company, in EUR, for the /companies info tooltip.
--
-- GuruFocus reports market cap in the stock's native (exchange) currency; the
-- backfill (`index_universe/backfill_market_cap.py`) converts it to EUR at the
-- latest FX rate and stamps the capture date. It's a point-in-time snapshot —
-- re-run the backfill to refresh. Nullable: companies GuruFocus can't price
-- (out-of-scope regions) or whose currency has no FX rate stay NULL.
ALTER TABLE public.company ADD COLUMN IF NOT EXISTS market_cap_eur double precision;
ALTER TABLE public.company ADD COLUMN IF NOT EXISTS market_cap_date date;
