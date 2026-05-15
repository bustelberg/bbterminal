-- Align gurufocus_exchange.exchange_code for Asia + Middle East + remaining EM
-- rows with the URL-prefix convention used by acwi.py / ingest/prices.py.
--
-- 20260503000000 already did this for Europe (PRA->XPRA, ISE->DUB, BDP->BUD,
-- plus the Vienna/Prague swap). The Asia/ME/EM rows were left under their
-- original API-style codes (SSE, TWSE, GTSM, KLSE, IDX, SET, PSE, TADAWUL,
-- QSE, KSE, TASE, EGX, MCX, SGO). That mismatch caused every iShares ACWI
-- holding on those exchanges to silently skip during sync, because
-- exch_id_map.get("BKK") returned None even though a "SET" row meant the
-- same thing.
--
-- Codes here are produced by index_universe/acwi/exchange_map.gurufocus_exchange_for_db().
-- For the two URL->API conversions (MCX->MIC, TASE->XTAE) we use the API form
-- because that's what gurufocus_exchange_for_db() returns.

BEGIN;

-- Asia (East/SE/South)
UPDATE gurufocus_exchange SET exchange_code = 'SHSE' WHERE exchange_code = 'SSE';
UPDATE gurufocus_exchange SET exchange_code = 'TPE'  WHERE exchange_code = 'TWSE';
UPDATE gurufocus_exchange SET exchange_code = 'ROCO' WHERE exchange_code = 'GTSM';
UPDATE gurufocus_exchange SET exchange_code = 'XKLS' WHERE exchange_code = 'KLSE';
UPDATE gurufocus_exchange SET exchange_code = 'ISX'  WHERE exchange_code = 'IDX';
UPDATE gurufocus_exchange SET exchange_code = 'BKK'  WHERE exchange_code = 'SET';
UPDATE gurufocus_exchange SET exchange_code = 'PHS'  WHERE exchange_code = 'PSE';

-- Middle East
UPDATE gurufocus_exchange SET exchange_code = 'SAU'  WHERE exchange_code = 'TADAWUL';
UPDATE gurufocus_exchange SET exchange_code = 'DSMD' WHERE exchange_code = 'QSE';
UPDATE gurufocus_exchange SET exchange_code = 'KUW'  WHERE exchange_code = 'KSE';
UPDATE gurufocus_exchange SET exchange_code = 'XTAE' WHERE exchange_code = 'TASE';

-- Africa / LatAm / Russia (out of subscription scope, renamed for schema
-- consistency so the startup sanity check stays quiet)
UPDATE gurufocus_exchange SET exchange_code = 'CAI'  WHERE exchange_code = 'EGX';
UPDATE gurufocus_exchange SET exchange_code = 'MIC'  WHERE exchange_code = 'MCX';
UPDATE gurufocus_exchange SET exchange_code = 'XSGO' WHERE exchange_code = 'SGO';

COMMIT;
