-- User-entered Expected Growth Model inputs are research assumptions, not vendor fundamentals.
-- Keep them separate from `metric_data` so refreshes cannot replace them.
CREATE TABLE IF NOT EXISTS public.company_egm_assumption (
    company_id integer PRIMARY KEY REFERENCES public.company(company_id) ON DELETE CASCADE,
    eps_growth numeric,
    exit_pe numeric,
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT company_egm_assumption_eps_growth_valid
        CHECK (eps_growth IS NULL OR eps_growth > -1),
    CONSTRAINT company_egm_assumption_exit_pe_valid
        CHECK (exit_pe IS NULL OR exit_pe > 0),
    CONSTRAINT company_egm_assumption_has_value
        CHECK (eps_growth IS NOT NULL OR exit_pe IS NOT NULL)
);

ALTER TABLE public.company_egm_assumption ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS company_egm_assumption_deny_all ON public.company_egm_assumption;
CREATE POLICY company_egm_assumption_deny_all
    ON public.company_egm_assumption FOR ALL USING (false);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.company_egm_assumption TO service_role;

NOTIFY pgrst, 'reload schema';
