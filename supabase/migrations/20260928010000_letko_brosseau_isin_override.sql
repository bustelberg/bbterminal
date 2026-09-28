-- Durable identity fallback for the Letko Brosseau Global Emerging Markets Equity Fund.
--
-- Current Vermogensoverzicht snapshots carry IE000MEQP5U8 themselves, but older exports and an
-- AIRS row without the optional ISIN column must resolve to the same instrument in production.
-- AIRS currently abbreviates/misspells the manager as "Letko Bross"; retain that exact source
-- label as the primary key because transaction and dividend joins also use it.  The correctly
-- spelled alias protects a future AIRS label correction without requiring another deployment.
--
-- The resolver only consults this table when the book supplies no ISIN, so this seed can never
-- replace a custodian-provided identity silently.
INSERT INTO public.airs_holding_isin_override (holding_name, isin, note) VALUES
  ('Letko Bross Global EM Equity Fund', 'IE000MEQP5U8',
   'Verified Class Launch share class of the Letko Brosseau Global Emerging Markets Equity Fund, a sub-fund of Candoris ICAV. AIRS currently abbreviates the manager as Letko Bross.'),
  ('Letko Brosseau Global EM Equity Fund', 'IE000MEQP5U8',
   'Correctly spelled alias for the Class Launch share class of the Letko Brosseau Global Emerging Markets Equity Fund, a sub-fund of Candoris ICAV.')
ON CONFLICT (holding_name) DO UPDATE
SET isin = EXCLUDED.isin,
    note = EXCLUDED.note,
    updated_at = now();

NOTIFY pgrst, 'reload schema';
