ALTER TABLE public.airs_model_portfolio_position ADD COLUMN IF NOT EXISTS afm_categorie text;
ALTER TABLE public.airs_model_portfolio ADD COLUMN IF NOT EXISTS positions_schema_version smallint NOT NULL DEFAULT 1;
NOTIFY pgrst, 'reload schema';
