-- One-off data fix for four misrouted company-listing mappings flagged
-- by `/api/admin/companies/flagged` on 2026-05-30. Each fix is gated by
-- the *current wrong* (gurufocus_ticker, exchange_code, company_name)
-- triple so the migration is idempotent: a re-run finds nothing to
-- update and does nothing.
--
-- For the three remapped listings (CNOOC, Gold Fields, Verbund) we also
-- DELETE the old close_price rows in the same CTE — those prices were
-- scraped from the *wrong security* (a German depositary, a Prague
-- cross-listing, etc.) and would otherwise pollute the primary
-- listing's series until they aged out. The next ingest tick will
-- re-fetch prices from GuruFocus using the corrected (ticker, exchange)
-- pair and rebuild the metric_data series cleanly.
--
-- Each fix is wrapped in its own CTE so that one failing UPDATE doesn't
-- block the others; the RETURNING company_id → DELETE pattern fires the
-- DELETE only when the UPDATE actually changed a row.

-- ── CNOOC Ltd: Xetra depositary (NC2B) → HKSE primary (00883) ──
WITH cnooc_fix AS (
    UPDATE company
       SET gurufocus_ticker = '00883',
           exchange_id = (SELECT exchange_id FROM gurufocus_exchange WHERE exchange_code = 'HKSE')
     WHERE company_name = 'CNOOC Ltd'
       AND gurufocus_ticker = 'NC2B'
       AND exchange_id = (SELECT exchange_id FROM gurufocus_exchange WHERE exchange_code = 'XTER')
    RETURNING company_id
)
DELETE FROM metric_data
 WHERE company_id IN (SELECT company_id FROM cnooc_fix)
   AND metric_code = 'close_price';

-- ── Gold Fields Ltd: Xetra depositary (EDG) → NYSE primary (GFI) ──
-- Also drops the "(ADR)" suffix from the name since it's now the
-- direct NYSE listing rather than a depositary.
WITH gfi_fix AS (
    UPDATE company
       SET gurufocus_ticker = 'GFI',
           exchange_id = (SELECT exchange_id FROM gurufocus_exchange WHERE exchange_code = 'NYSE'),
           company_name = 'Gold Fields Ltd'
     WHERE company_name = 'Gold Fields Ltd (ADR)'
       AND gurufocus_ticker = 'EDG'
       AND exchange_id = (SELECT exchange_id FROM gurufocus_exchange WHERE exchange_code = 'XTER')
    RETURNING company_id
)
DELETE FROM metric_data
 WHERE company_id IN (SELECT company_id FROM gfi_fix)
   AND metric_code = 'close_price';

-- ── VERBUND AG: Prague cross-listing (XPRA) → Vienna primary (WBO) ──
WITH verbund_fix AS (
    UPDATE company
       SET exchange_id = (SELECT exchange_id FROM gurufocus_exchange WHERE exchange_code = 'WBO')
     WHERE company_name = 'VERBUND AG'
       AND gurufocus_ticker = 'VER'
       AND exchange_id = (SELECT exchange_id FROM gurufocus_exchange WHERE exchange_code = 'XPRA')
    RETURNING company_id
)
DELETE FROM metric_data
 WHERE company_id IN (SELECT company_id FROM verbund_fix)
   AND metric_code = 'close_price';

-- ── Mcphy Energy SA: stamp out_of_scope rather than remap ──
-- Dead/illiquid US OTC listing of a French hydrogen company that's
-- effectively delisted. The Leonteq + ACWI template-refresh filters
-- already exclude out_of_scope rows from membership, so on the next
-- refresh Mcphy drops out of every universe automatically. The row
-- stays for /companies (will show the OUT OF SCOPE badge) so the
-- audit trail is preserved.
UPDATE company
   SET out_of_scope_at = NOW(),
       out_of_scope_reason = 'Manual review (2026-05-30): dead/illiquid OTC listing; primary delisted'
 WHERE company_name = 'Mcphy Energy SA'
   AND gurufocus_ticker = 'MPHYF'
   AND exchange_id = (SELECT exchange_id FROM gurufocus_exchange WHERE exchange_code = 'NYSE')
   AND out_of_scope_at IS NULL;

NOTIFY pgrst, 'reload schema';
