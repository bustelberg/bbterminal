-- Does this instrument have a revenue series at all?
--
-- Same three-valued discipline as has_payments / has_dividend_payments:
--     NULL   never fetched         -> the cell offers "Fetch"
--     true   has revenue           -> "View"
--     false  fetched, none exists  -> "NO DATA"
-- A plain boolean collapses "we have not looked" into "there is nothing", which is the
-- blank-cell lie every one of these columns exists to avoid.
--
-- MOST GRID ROWS NEED NO API CALL TO ANSWER THIS. Revenue is a property of an operating
-- business, so it is meaningless for ~59% of the grid and we never ask:
--     BONDS   4,877 | FUTURE 410 | FX/CRYPTO 2   -> not an equity at all
--     ETF     2,507 | FUNDS  1,681               -> a fund HOLDS securities, it does not
--                                                   operate a business (GuruFocus agrees:
--                                                   stock/QQQ/financials returns null)
-- Only the ~6,630 EQUITY rows can have one.
--
-- NOTE for whoever charts this next: GuruFocus serves financials in the LISTING's
-- trading currency, NOT the company's reporting currency, converting per fiscal period.
-- CSX reports USD; its Xetra line (XTER:CXR) comes back in EUR:
--     FY2024-12   Nasdaq 14,540 USD   Xetra 13,885.700 EUR   (x0.955 = the 2024 rate)
--     FY2025-12   Nasdaq 14,092 USD   Xetra 12,034.568 EUR   (x0.854 = the 2025 rate)
-- That is the OPPOSITE of the dividend feed, which reports the declaration currency on
-- every listing (Apple = 0.27 USD on Nasdaq, Xetra, Zurich and Milan alike). So for
-- REVENUE the choice of listing changes the number, and `is_home` earns its keep.
-- `summary.company_data` distinguishes the two: `currency` (trading) vs `currency_comp`
-- (reporting).

ALTER TABLE public.gurufocus_listing
    ADD COLUMN IF NOT EXISTS has_revenue boolean;

ALTER TABLE public.company
    ADD COLUMN IF NOT EXISTS has_revenue boolean;

NOTIFY pgrst, 'reload schema';
