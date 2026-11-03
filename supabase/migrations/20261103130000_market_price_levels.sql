CREATE TABLE public.market_price_level (
  user_id uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  market text NOT NULL CHECK (market IN ('nasdaq', 'acwi', 'iwqu')),
  drawdown_pct numeric(5,2) NOT NULL CHECK (drawdown_pct > 0 AND drawdown_pct < 100),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, market, drawdown_pct)
);
ALTER TABLE public.market_price_level ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.market_price_level FROM anon, authenticated;
GRANT SELECT, INSERT, DELETE ON public.market_price_level TO authenticated;
GRANT ALL ON public.market_price_level TO service_role;
CREATE POLICY market_levels_select_own ON public.market_price_level FOR SELECT TO authenticated
  USING (user_id = (SELECT auth.uid()));
CREATE POLICY market_levels_insert_own ON public.market_price_level FOR INSERT TO authenticated
  WITH CHECK (user_id = (SELECT auth.uid()));
CREATE POLICY market_levels_delete_own ON public.market_price_level FOR DELETE TO authenticated
  USING (user_id = (SELECT auth.uid()));
NOTIFY pgrst, 'reload schema';
