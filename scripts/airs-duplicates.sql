-- Duplicate rows in the AIRS scrape tables, by NATURAL key.
--
-- These four tables have a surrogate `id` primary key and no natural unique constraint, and both
-- local and prod scrape AIRS independently -- each assigning its own serial. The clone upserted
-- local's rows BY PK, so a local row whose id happened to be free on prod was INSERTED beside the
-- row prod already held for the same holding. Nothing removed it afterwards, because these tables
-- are "additive" (never delete prod-only rows). This counts the damage.
--
-- READ-ONLY. Run against prod:
--   docker exec -e PGPASSWORD=... -i supabase_db_bbterminal psql "<PROD_URL>" -f - < scripts/airs-duplicates.sql
--
-- `groups`      = natural keys that appear more than once
-- `extra_rows`  = rows to delete to make each key unique again (total - distinct)
--
-- !! `airs_model_portfolio_position` INCLUDES `percentage` AND `fonds` IN THE KEY ON PURPOSE. A
-- model may legitimately hold the same instrument twice at different weights -- VTopSelectie OFF FX
-- holds CapitaLand at 2% and at 3% -- so keying on (portfolio_id, datum, isin) alone would report
-- a real composition as a duplicate. Two rows identical in every one of these fields are a copy.
-- !! THE KEY FOR airs_holding INCLUDES quantity AND current_value_eur, AND WITHOUT THEM THIS
-- QUERY LIES. (portefeuille, as_of_date, holding_name) is NOT unique in AIRS: measured on local,
-- 83 pairs share it -- e.g. "6,5% Rabobank Certificaten 14-perp." appears twice in one book on one
-- date at EUR 8,347.20 and EUR 112.23, both carrying the SAME `retrieved_at`. Same scrape, two
-- genuine lines (the position and its accrued interest) that happen to share a display name.
--
-- !! AND `retrieved_at` IS DELIBERATELY *NOT* IN THE KEY. A clone-inserted copy carries the
-- timestamp of the OTHER side's scrape, so an all-columns comparison would miss exactly the rows
-- this query exists to find. The key is the DATA: same book, same date, same instrument, same
-- quantity, same value = one holding stored twice.
SELECT 'airs_holding' AS tbl,
       count(*) FILTER (WHERE n > 1) AS groups,
       coalesce(sum(n - 1) FILTER (WHERE n > 1), 0) AS extra_rows
FROM (SELECT count(*) AS n FROM public.airs_holding
      GROUP BY portefeuille, as_of_date, holding_name, quantity, current_value_eur) q
UNION ALL
SELECT 'airs_mutatie',
       count(*) FILTER (WHERE n > 1),
       coalesce(sum(n - 1) FILTER (WHERE n > 1), 0)
FROM (SELECT count(*) AS n FROM public.airs_mutatie
      GROUP BY portefeuille, boekdatum, grootboek, fonds, omschrijving, amount_eur) q
UNION ALL
SELECT 'airs_model_portfolio',
       count(*) FILTER (WHERE n > 1),
       coalesce(sum(n - 1) FILTER (WHERE n > 1), 0)
FROM (SELECT count(*) AS n FROM public.airs_model_portfolio GROUP BY name) q
UNION ALL
SELECT 'airs_model_portfolio_position',
       count(*) FILTER (WHERE n > 1),
       coalesce(sum(n - 1) FILTER (WHERE n > 1), 0)
FROM (SELECT count(*) AS n FROM public.airs_model_portfolio_position
      GROUP BY portfolio_id, datum, isin, fonds, percentage) q
ORDER BY 3 DESC;
