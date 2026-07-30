-- AIRS's own per-holding columns from the Vermogensoverzicht (VOLK) export.
--
-- We were parsing 7 of the sheet's 13 columns and RECOMPUTING what AIRS already
-- publishes. These six carry AIRS's own figures, stored AS REPORTED beside ours:
--
--   kostprijs lopend jaar -> cost_basis_local
--   huidige koers         -> current_price_local
--   weging                -> airs_weight        (cf. our computed `weight`)
--   fondsresultaat        -> fund_result_eur    (the PERFORMANCE leg)
--   valutaresultaat       -> fx_result_eur      (the FX leg)
--   resultaat in %        -> airs_result_pct    (cf. our computed `ytd_return_pct`)
--
-- fund_result_eur/fx_result_eur are the prize: the split of a result into performance
-- and FX, which nothing we compute can produce. Our own ytd_return_pct (EUR) and
-- ytd_return_local_pct bracket the FX leg but never isolate it.
--
-- ⚠ THE UNITS ARE MEASURED, NOT INFERRED — all from a real download of
-- BUS_MTS_OFF_AFS_DYN (2026-01-01..2026-07-16), row `Visa`:
--
--   * `airs_result_pct` is a PERCENT and is the EUR return: Visa reads 11.41, and
--     (38211.21 - 34298.74)/34298.74 = 0.1141 -- i.e. EXACTLY 100x our neighbouring
--     `ytd_return_pct`, which is a FRACTION. Two adjacent columns, one name, 100x apart.
--     Nothing rescales either; they are carried as a cross-check.
--   * `airs_weight` is likewise a PERCENT (Visa 5.46) where our `weight` is a fraction.
--   * fund_result_eur/fx_result_eur are in EUR: 3099 + 813.18 = 3912.18, against the EUR
--     delta 38211.21 - 34298.74 = 3912.47 (to rounding). They are NOT local -- the local
--     delta is 3553.96, which matches neither leg.
--
-- All nullable: an export predating these columns must still store.
alter table airs_holding
  add column if not exists cost_basis_local     numeric,
  add column if not exists current_price_local  numeric,
  add column if not exists airs_weight          numeric,
  add column if not exists fund_result_eur      numeric,
  add column if not exists fx_result_eur        numeric,
  add column if not exists airs_result_pct      numeric;

comment on column airs_holding.cost_basis_local is
  'AIRS `Kostprijs lopend jaar`, as reported (holding currency).';
comment on column airs_holding.current_price_local is
  'AIRS `Huidige koers` — price per share in the holding currency, as reported.';
comment on column airs_holding.airs_weight is
  'AIRS `Weging`, as reported — a PERCENT (5.46). Our own `weight` is a fraction (0.0546).';
comment on column airs_holding.fund_result_eur is
  'AIRS `Fondsresultaat` — the PERFORMANCE leg of the EUR result. Measured: fund+fx = the '
  'EUR value delta.';
comment on column airs_holding.fx_result_eur is
  'AIRS `Valutaresultaat` — the FX leg of the EUR result.';
comment on column airs_holding.airs_result_pct is
  'AIRS `Resultaat in %`, as reported — a PERCENT (11.41), and the EUR return. NOT the same '
  'unit as ytd_return_pct, which is the same number as a fraction (0.1141). Carried beside '
  'ours as a cross-check; neither is rescaled into the other.';
