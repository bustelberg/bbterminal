-- Experian plc's verified LSE line was already present as company #85, but
-- lacked its ISIN.  That left GB00B19NLV48 unlinked in asset_grid and hid
-- company-scoped features such as investment Notes from portfolio analysis.
UPDATE company AS c
SET isin = 'GB00B19NLV48'
FROM gurufocus_exchange AS e
WHERE c.exchange_id = e.exchange_id
  AND c.gurufocus_ticker = 'EXPN'
  AND e.exchange_code = 'LSE'
  AND c.isin IS DISTINCT FROM 'GB00B19NLV48';
