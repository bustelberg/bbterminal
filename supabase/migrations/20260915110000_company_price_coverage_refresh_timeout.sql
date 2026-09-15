-- This refresh aggregates the complete GuruFocus price history. It can exceed
-- the API role's normal statement timeout while still making valid progress.
ALTER FUNCTION public.refresh_company_price_coverage()
  SET statement_timeout = '5min';
