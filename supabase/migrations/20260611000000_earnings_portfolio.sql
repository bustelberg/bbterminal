-- Earnings-dashboard portfolios: user-defined baskets of existing companies
-- with weights, used by the /earnings A-vs-B comparison's "portfolio mode".
--
-- A portfolio is a named list of company_ids + weights. The dashboard
-- aggregates each member's metric_data into a single synthesized MetricRow[]
-- (weighted mean per metric per date, EUR-normalized) so every existing
-- earnings chart renders a portfolio exactly like a single company.
--
-- NOTE: distinct from the existing `portfolio` / `portfolio_weight` tables,
-- which are the AIRS published-portfolio concept (portfolio_name + target_date
-- + published_at). These are unrelated — hence the `earnings_` prefix.
--
-- Backend reads/writes via the service-role key, so (like panel_cache) the
-- tables are RLS deny-all with service_role GRANTs only; users never hit them
-- directly — they go through /api/earnings/portfolios.

CREATE TABLE public.earnings_portfolio (
    id bigserial PRIMARY KEY,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.earnings_portfolio_member (
    portfolio_id bigint NOT NULL REFERENCES public.earnings_portfolio(id) ON DELETE CASCADE,
    company_id integer NOT NULL REFERENCES public.company(company_id),
    -- Raw weight; normalized (to sum 1, and to present-members per date) at
    -- aggregation time. Non-negative.
    weight double precision NOT NULL DEFAULT 0,
    PRIMARY KEY (portfolio_id, company_id),
    CONSTRAINT earnings_portfolio_member_weight_nonneg CHECK (weight >= 0)
);

-- FK lookup index (the linter flags unindexed FKs — see the 2026-05-22 sweep).
CREATE INDEX earnings_portfolio_member_company_id_idx
    ON public.earnings_portfolio_member (company_id);

-- RLS: deny-all default + service-role grants, matching the project pattern
-- (panel_cache). All access is via the backend service key.
ALTER TABLE public.earnings_portfolio ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.earnings_portfolio_member ENABLE ROW LEVEL SECURITY;
CREATE POLICY earnings_portfolio_deny_all ON public.earnings_portfolio FOR ALL USING (false);
CREATE POLICY earnings_portfolio_member_deny_all ON public.earnings_portfolio_member FOR ALL USING (false);

GRANT SELECT, INSERT, UPDATE, DELETE ON public.earnings_portfolio TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.earnings_portfolio_member TO service_role;
GRANT USAGE, SELECT ON SEQUENCE public.earnings_portfolio_id_seq TO service_role;
