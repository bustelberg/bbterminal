-- The four ATT (Rendementen) columns we never parsed.
--
-- The sheet has TWELVE columns; `_parse_att_excel` read seven. Measured on a real
-- download of BUS_MTS_OFF_AFS_DYN (2026-01-01..2026-07-16):
--
--     Periode · Beginvermogen · Stortingen · Onttrekkingen · Koersresultaat ·
--     Opbrengsten · Kosten · Mutatie opgelopen rente · Beleggingsresultaat ·
--     Eindvermogen · Rendement · Cumulatief rendement
--
-- ⚠ `stortingen`/`onttrekkingen` are the account's FLOWS. They are NOT, as first
-- assumed here, the reason `rendement` and `cumulatief_rendement` disagree: measured,
-- AITopSelectie OFF DYN has both = 0 in every month of 2026 and its two figures still
-- differ by 50pp. They differ because one ATT row is one MONTH (see _airs_accounts.
-- _year_perf). The flows matter for a different reason — they are the term that makes
-- the year's result identity close:
--
--     eindvermogen - beginvermogen - stortingen + onttrekkingen == sum(beleggingsresultaat)
--
-- verified to a residual of -0.00 on AITopSelectie (422,087.64 both sides).
--
-- ⚠ `mutatie_opgelopen_rente` closes the per-row result:
--
--     beleggingsresultaat = koersresultaat + opbrengsten + mutatie_opgelopen_rente   (+/- kosten)
--
-- BUS_BepOffensief_Dyn: -1358.33 + 1734.67 + 21.37 = 397.71, exactly as reported —
-- the 21.37 that did not add up before was accrued interest, not costs.
--
-- ⚠ THE SIGN OF `kosten` IS UNVERIFIED. It is 0.00 on every portfolio measured so far,
-- so `- kosten` and `+ kosten` are indistinguishable in the data and the identity above
-- cannot choose between them. Stored as reported; do not put it into arithmetic until a
-- portfolio with non-zero costs has been read.
--
-- All nullable: rows stored before this migration have no values for them, and 0
-- would be a claim (a book with no recorded costs and one whose costs we never read
-- are not the same fact).
alter table airs_performance
  add column if not exists stortingen               numeric,
  add column if not exists onttrekkingen            numeric,
  add column if not exists kosten                   numeric,
  add column if not exists mutatie_opgelopen_rente  numeric;

comment on column airs_performance.stortingen is
  'AIRS `Stortingen` — deposits into the account over the period, EUR. With onttrekkingen, '
  'this is why rendement (the value ratio) and cumulatief_rendement disagree.';
comment on column airs_performance.onttrekkingen is
  'AIRS `Onttrekkingen` — withdrawals from the account over the period, EUR.';
comment on column airs_performance.kosten is
  'AIRS `Kosten` — costs charged over the period, EUR.';
comment on column airs_performance.mutatie_opgelopen_rente is
  'AIRS `Mutatie opgelopen rente` — change in accrued interest over the period, EUR. With '
  'kosten, closes: beleggingsresultaat = koersresultaat + opbrengsten - kosten + this.';
