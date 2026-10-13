-- AEN000101016 is First Abu Dhabi Bank PJSC (ADX:FAB), not the unrelated
-- US-listed First Trust Multi Cap Value AlphaDEX Fund (NASDAQ:FAB). Yahoo has
-- no ADX series for the bank, so retaining the bare FAB mapping invents both a
-- price history and an ETF classification. The checked-in Financials override
-- keeps the known constituent classified while this removes the false listing.
UPDATE asset_execution
SET status = 'not_found',
    is_default = FALSE,
    reason = 'unmapped by hand: First Abu Dhabi Bank PJSC (AEN000101016) has no Yahoo ADX listing; bare FAB is an unrelated US ETF',
    yahoo_symbol = NULL,
    analysis_id = NULL,
    asset_class = NULL,
    exchange = NULL,
    currency = NULL,
    listing_country = NULL,
    med_adv_eur = NULL,
    first_date = NULL,
    years = NULL,
    name = COALESCE(openfigi_name, name),
    identity_status = 'unknown',
    updated_at = NOW()
WHERE isin = 'AEN000101016'
  AND yahoo_symbol = 'FAB'
  AND openfigi_name = 'FIRST ABU DHABI BANK PJSC';
