-- A STARTING allocation policy, so the grid opens with something to react to instead of empty.
--
-- ⚠⚠ THESE NUMBERS ARE A PLACEHOLDER SHAPE, NOT ADVICE, AND NOTHING MEASURED THEM. They were
--   written to be adjusted: equities fall and bonds rise monotonically across the four profiles,
--   the direct-vs-ETF split follows the ratio the live books happen to show (BUS_Offensief_Dyn is
--   ~8:1), and each profile's defaults total 98% so a ~2% cash sleeve is the remainder. That is a
--   plausible ladder, not this firm's mandate — read every cell before treating it as policy.
--
-- ⚠ ON CONFLICT DO NOTHING, WHICH IS THE ENTIRE POINT OF SEEDING IT THIS WAY. Once somebody edits
--   a cell, this migration must never speak again — re-running it on an environment where the
--   policy has been tuned would quietly reinstate the placeholder over a real decision. It fills
--   gaps; it does not correct anything.
--
-- ⚠ THE MAXIMA DELIBERATELY SUM TO MORE THAN 100 AND THE MINIMA TO LESS. A band is a permitted
--   range per class, not a share of a partition — requiring the extremes to add to 100 would mean
--   every class is at its limit simultaneously, which is a single portfolio, not a band.
INSERT INTO public.airs_allocation_band (variant, bucket, min_pct, default_pct, max_pct) VALUES
    -- Offensief — equity-dominant; bonds present only as ballast.
    ('Offensief',         'Equity',        65,  80,  90),
    ('Offensief',         'Equity ETF',     0,  10,  25),
    ('Offensief',         'Bonds',          0,   5,  15),
    ('Offensief',         'Alternatives',   0,   3,  10),
    -- Beperkt Offensief — still equity-led, with a real fixed-income sleeve.
    ('Beperkt Offensief', 'Equity',        50,  62,  75),
    ('Beperkt Offensief', 'Equity ETF',     0,  10,  25),
    ('Beperkt Offensief', 'Bonds',         10,  23,  35),
    ('Beperkt Offensief', 'Alternatives',   0,   3,  10),
    -- Neutraal — roughly balanced between equities and bonds.
    ('Neutraal',          'Equity',        30,  45,  58),
    ('Neutraal',          'Equity ETF',     0,   8,  20),
    ('Neutraal',          'Bonds',         30,  42,  55),
    ('Neutraal',          'Alternatives',   0,   3,  10),
    -- Defensief — bond-dominant; equities are the satellite.
    ('Defensief',         'Equity',        10,  25,  35),
    ('Defensief',         'Equity ETF',     0,   5,  15),
    ('Defensief',         'Bonds',         55,  65,  80),
    ('Defensief',         'Alternatives',   0,   3,  10)
ON CONFLICT (variant, bucket) DO NOTHING;
