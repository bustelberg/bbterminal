-- Historical MomentumTopSelectie sales precede retained AIRS holding snapshots.
-- These abbreviated AIRS transaction names cannot be matched conservatively to
-- the company universe, so pin their verified identities for sold-risk history.
INSERT INTO public.airs_holding_isin_override (holding_name, isin, note) VALUES
  ('RingCentral A', 'US76680R2067', 'Historical MomentumTopSelectie sold row; RingCentral Inc.'),
  ('Wabtec Corporation', 'US9297401088', 'Historical MomentumTopSelectie sold row; Westinghouse Air Brake Technologies / Wabtec.'),
  ('Panasonic', 'JP3866800000', 'Historical MomentumTopSelectie sold row; Panasonic Holdings.'),
  ('Dai-ichi Life Insurance', 'JP3476480003', 'Historical MomentumTopSelectie sold row; Dai-ichi Life Group.'),
  ('Travelers Companies', 'US89417E1091', 'Historical MomentumTopSelectie sold row; The Travelers Companies.')
ON CONFLICT (holding_name) DO UPDATE SET isin = EXCLUDED.isin, note = EXCLUDED.note;

NOTIFY pgrst, 'reload schema';
