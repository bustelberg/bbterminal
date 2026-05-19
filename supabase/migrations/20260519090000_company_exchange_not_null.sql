-- Enforce that every `company` row has an exchange_id. NULL-exchange
-- companies were the root cause of empty exchange columns in /backtest
-- and /schedule, plus broken GuruFocus links (frontend falls back to a
-- bare-ticker US URL that 404s for non-US securities). Going forward
-- the only place an exchange_id can be set is the CRUD endpoints in
-- `backend/routers/companies.py`, which now reject empty / unresolvable
-- exchange codes at the API layer.
--
-- Pre-flight: if any `company.exchange_id` is NULL when this migration
-- runs, FAIL with a clear pointer to the admin endpoint that fixes
-- them. We deliberately don't auto-coerce — silently picking a default
-- exchange would let bad data through (the ENI-on-MIL story is exactly
-- the kind of breakage that loud-fail catches and silent-fix hides).
DO $$
DECLARE
    null_count integer;
    sample_tickers text;
BEGIN
    SELECT COUNT(*) INTO null_count FROM company WHERE exchange_id IS NULL;
    IF null_count > 0 THEN
        SELECT string_agg(gurufocus_ticker, ', ') INTO sample_tickers FROM (
            SELECT gurufocus_ticker FROM company
            WHERE exchange_id IS NULL
            ORDER BY company_name
            LIMIT 10
        ) s;
        RAISE EXCEPTION
            'Cannot add NOT NULL on company.exchange_id: % row(s) have NULL exchange_id. Sample: %. Run POST /api/admin/companies/resolve-missing-exchanges?dry_run=false to bulk-resolve via OpenFIGI, or fix manually in /companies, then re-apply this migration.',
            null_count, sample_tickers;
    END IF;
END $$;

ALTER TABLE company ALTER COLUMN exchange_id SET NOT NULL;
