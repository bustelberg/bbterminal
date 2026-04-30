-- Align gurufocus_exchange.exchange_code with the URL-prefix convention used
-- by acwi.py / ingest/prices.py. The original seed (20260418000000) used what
-- looked like canonical "API codes" (e.g. ISE, BDP, PRA), but every code path
-- that talks to GuruFocus -- price/volume fetches, the URL builder in
-- acwi.gurufocus_url, fill_ticker.json overrides -- uses the URL-prefix form
-- (DUB, BUD, XPRA, etc.). The mismatch caused every iShares holding from
-- those exchanges to silently skip during ACWI sync because exch_id_map.get()
-- returned None.
--
-- Two small twists in the existing seed:
--   * 'XPRA' was labelled 'Vienna Stock Exchange' (XPRA is Prague's code).
--   * The proper Vienna code 'WBO' was missing entirely.
-- We fix both by renaming the existing XPRA row to WBO (it had the right
-- country/currency for Vienna), then renaming PRA -> XPRA so Prague has the
-- correct code.

BEGIN;

-- 1. The misnamed 'XPRA' (currently Vienna AT/EUR) becomes 'WBO'.
UPDATE gurufocus_exchange
SET exchange_code = 'WBO',
    exchange_name = 'Wiener Boerse'
WHERE exchange_code = 'XPRA';

-- 2. 'PRA' (Prague) becomes 'XPRA' -- now matches GF URLs like XPRA:KOMB.
UPDATE gurufocus_exchange SET exchange_code = 'XPRA' WHERE exchange_code = 'PRA';

-- 3. Other European codes that didn't match the URL convention.
UPDATE gurufocus_exchange SET exchange_code = 'DUB' WHERE exchange_code = 'ISE';
UPDATE gurufocus_exchange SET exchange_code = 'BUD' WHERE exchange_code = 'BDP';

COMMIT;
