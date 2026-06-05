-- Smart, dependency-driven scheduled pipeline: per-run derived plan.
--
-- The `smart_daily` orchestrator derives, from the set of enabled
-- scheduled strategies, exactly which universes need refreshing and which
-- strategies are due to rebalance on this tick — then runs only that work.
-- `plan_summary` records that derived plan (needed templates, resolved
-- universes, the per-strategy due decision, scoped company counts, and any
-- unresolved universe labels) so /schedule can show what the pipeline
-- decided to do and why, without re-deriving it client-side.
--
-- Nullable + additive: the legacy full/bootstrap orchestrators never write
-- it, and existing rows keep NULL.

ALTER TABLE public.ingest_run
    ADD COLUMN IF NOT EXISTS plan_summary jsonb;

COMMENT ON COLUMN public.ingest_run.plan_summary IS 'Smart-pipeline derived plan for this run: {as_of, needed_template_keys[], unresolved_labels[], due_strategy_ids[], universes_refreshed[], held_company_count, universe_company_count, strategies:[{strategy_id, strategy_name, frequency, rebalance_weekday, label, resolved_template_key, resolved_universe_id, is_due, due_reason}]}. NULL on legacy full/bootstrap runs that do not derive a plan.';
