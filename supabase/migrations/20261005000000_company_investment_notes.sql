-- Personal investment hypotheses belong to the signed-in user, never to the
-- vendor-owned company record. A colleague can therefore research the same
-- company without seeing or overwriting another user's thesis.
CREATE TABLE IF NOT EXISTS public.company_investment_note (
    company_id integer NOT NULL REFERENCES public.company(company_id) ON DELETE CASCADE,
    user_id uuid NOT NULL,
    user_email text,
    thesis text NOT NULL DEFAULT '',
    pillars jsonb NOT NULL DEFAULT '[]'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (company_id, user_id),
    CONSTRAINT company_investment_note_pillars_array CHECK (jsonb_typeof(pillars) = 'array')
);

REVOKE ALL ON public.company_investment_note FROM anon, authenticated;
GRANT ALL ON public.company_investment_note TO service_role;

COMMENT ON TABLE public.company_investment_note IS
  'Private, user-authored company investment theses and supporting pillars.';

NOTIFY pgrst, 'reload schema';
