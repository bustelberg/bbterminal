-- One database round trip for the management-dashboard overview.
--
-- The Python overview used to compose this payload from 17 sequential PostgREST reads.  Those
-- reads were individually small, but their network latency dominated the page.  This function
-- keeps the monthly-performance and newest-holdings rules in one snapshot, then returns the
-- compact inputs the application still needs for its reviewed, file-backed strategy naming map.
CREATE OR REPLACE FUNCTION public.airs_overview_payload()
RETURNS TABLE (payload jsonb)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
WITH perf_year AS (
  SELECT p.*,
         max(extract(year FROM p.periode)) OVER (PARTITION BY p.portefeuille) AS report_year
  FROM airs_performance p
),
-- A daily scan may store several looks at the still-open month.  Keep the last one, exactly as
-- `_year_perf` did, before adding yearly money columns together.
perf_month AS (
  SELECT DISTINCT ON (portefeuille, date_trunc('month', periode)) *
  FROM perf_year
  WHERE extract(year FROM periode) = report_year
  ORDER BY portefeuille, date_trunc('month', periode), periode DESC, fetched_at DESC
),
perf_rollup AS (
  SELECT portefeuille,
         (array_agg(beginvermogen ORDER BY periode ASC, fetched_at ASC))[1] AS begin_value_eur,
         (array_agg(eindvermogen ORDER BY periode DESC, fetched_at DESC))[1] AS end_value_eur,
         (array_agg(cumulatief_rendement ORDER BY periode DESC, fetched_at DESC))[1] AS ytd_pct,
         (array_agg(rendement ORDER BY periode DESC, fetched_at DESC))[1] AS latest_month_pct,
         (array_agg(periode ORDER BY periode DESC, fetched_at DESC))[1] AS periode,
         count(*)::integer AS months,
         sum(coalesce(koersresultaat, 0)) AS price_result_eur,
         sum(coalesce(opbrengsten, 0)) AS income_eur,
         sum(coalesce(beleggingsresultaat, 0)) AS investment_result_eur,
         sum(coalesce(kosten, 0)) AS costs_eur,
         sum(coalesce(mutatie_opgelopen_rente, 0)) AS accrued_interest_change_eur,
         sum(coalesce(stortingen, 0)) AS deposits_eur,
         sum(coalesce(onttrekkingen, 0)) AS withdrawals_eur
  FROM perf_month
  GROUP BY portefeuille
),
perf AS (
  SELECT p.*,
         CASE WHEN begin_value_eur IS NULL OR end_value_eur IS NULL THEN NULL
              ELSE round(end_value_eur - begin_value_eur - deposits_eur + withdrawals_eur
                         - investment_result_eur, 2)
         END AS residual_eur
  FROM perf_rollup p
),
holding_dates AS (
  SELECT portefeuille, max(as_of_date) AS as_of
  FROM airs_holding
  GROUP BY portefeuille
),
holding_counts AS (
  SELECT h.portefeuille, d.as_of::text AS as_of,
         count(*)::integer AS holdings,
         count(DISTINCT nullif(btrim(h.isin), '')) FILTER (WHERE h.isin IS NOT NULL)::integer AS isins
  FROM holding_dates d
  JOIN airs_holding h ON h.portefeuille = d.portefeuille AND h.as_of_date = d.as_of
  GROUP BY h.portefeuille, d.as_of
),
roster_max AS (SELECT max(last_seen_at) AS last_seen_at FROM airs_account_roster),
accounts AS (
  SELECT p.portefeuille,
         p.periode::text AS periode,
         h.as_of,
         p.begin_value_eur, p.end_value_eur, p.ytd_pct, p.latest_month_pct, p.months,
         p.price_result_eur, p.income_eur, p.investment_result_eur, p.costs_eur,
         p.accrued_interest_change_eur, p.deposits_eur, p.withdrawals_eur, p.residual_eur,
         CASE WHEN p.residual_eur IS NULL THEN NULL ELSE abs(p.residual_eur) < 1 END AS reconciles,
         h.holdings, h.isins,
         r.reports_at::text AS fetched_at,
         CASE WHEN r.reports_at IS NULL THEN ARRAY[]::text[]
              ELSE ARRAY(SELECT report FROM unnest(ARRAY['att', 'volk', 'mut', 'model']) report
                         WHERE NOT (report = ANY(coalesce(r.reports_ok, ARRAY[]::text[]))))
         END AS missing_reports,
         adn.display_name AS account_display_name,
         aml.model_portfolio_id AS stored_model_portfolio_id,
         (aml.id IS NOT NULL) AS has_stored_link,
         aml.note AS stored_link_note
  FROM perf p
  LEFT JOIN holding_counts h ON h.portefeuille = p.portefeuille
  LEFT JOIN airs_account_roster r ON lower(r.portefeuille) = lower(p.portefeuille)
  LEFT JOIN airs_account_display_name adn ON lower(adn.portefeuille) = lower(p.portefeuille)
  LEFT JOIN airs_account_model_link aml ON lower(aml.portefeuille) = lower(p.portefeuille)
  WHERE NOT EXISTS (SELECT 1 FROM airs_account_hidden hidden
                    WHERE lower(hidden.portefeuille) = lower(p.portefeuille))
    AND (NOT EXISTS (SELECT 1 FROM airs_account_roster)
         OR EXISTS (SELECT 1 FROM airs_account_roster live, roster_max m
                    WHERE lower(live.portefeuille) = lower(p.portefeuille)
                      AND live.last_seen_at = m.last_seen_at))
),
models AS (
  SELECT m.id, m.name, m.display_name, m.omschrijving, m.portfolio_type,
         m.scanned_at::text AS scanned_at, m.positions_scanned_at::text AS positions_scanned_at,
         count(pos.id)::integer AS positions
  FROM airs_model_portfolio m
  LEFT JOIN airs_model_portfolio_position pos ON pos.portfolio_id = m.id
  GROUP BY m.id
)
SELECT jsonb_build_object(
  'accounts', COALESCE((SELECT jsonb_agg(to_jsonb(a) ORDER BY lower(a.portefeuille)) FROM accounts a), '[]'::jsonb),
  'models', COALESCE((SELECT jsonb_agg(to_jsonb(m) ORDER BY lower(m.name)) FROM models m), '[]'::jsonb)
);
$$;

REVOKE ALL ON FUNCTION public.airs_overview_payload() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.airs_overview_payload() TO service_role;

NOTIFY pgrst, 'reload schema';
