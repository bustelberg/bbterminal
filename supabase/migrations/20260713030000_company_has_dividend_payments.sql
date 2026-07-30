-- Does this company pay a dividend AT ALL?
--
-- The /asset-pipeline dividend view is now ONE primitive for every instrument: a
-- timeseries of (date, cash per unit held), straight from `stock/{sym}/dividend`.
-- Stocks and ETFs have exactly that and nothing else in common — the fiscal-period
-- series (annuals__Per Share Data__Dividends per Share) is company-only, is DERIVED
-- (payments summed inside a fiscal year), and lags up to a year, so it is no longer
-- what the chart shows.
--
-- That left the cell states asymmetric: an ETF that pays nothing could be badged
-- NO PAYOUTS from `gurufocus_listing.has_payments`, but a non-paying STOCK had no
-- equivalent and fell back to an inviting "Fetch" that would find nothing, forever.
-- Same fact, same badge, so companies get the same three-valued flag.
--
-- THREE-VALUED, deliberately:
--     NULL   never fetched         -> the cell offers "Fetch"
--     true   pays                  -> "View"
--     false  fetched, pays nothing -> "NO PAYOUTS"
-- A plain boolean would collapse "we have not looked" into "there is nothing to
-- find" — which is the blank-cell lie this whole column exists to avoid.

ALTER TABLE public.company
    ADD COLUMN IF NOT EXISTS has_dividend_payments boolean;

NOTIFY pgrst, 'reload schema';
