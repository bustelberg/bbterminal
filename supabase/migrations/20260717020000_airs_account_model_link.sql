-- Which MODEL is a given AIRS ACCOUNT running?
--
-- The two are separate scrapes of separate AIRS screens, and each holds exactly what the
-- other lacks:
--
--   Overzicht Modelportefeuilles -> airs_model_portfolio(_position)   weights + ISINCode,
--                                                                     but AIRS values none of it
--   Front-Office internal ptfs   -> airs_performance / airs_holding   real money, real returns,
--                                                                     but NO ISIN — only a name
--
-- Measured: of 58 models with a composition and 31 AIRS-valued accounts, the overlap is ZERO —
-- a portfolio has one or the other, never both. So the pairing is the only bridge between the
-- ISINs and the money, and it cannot be derived from the data on either side.
--
-- ⚠ THE HOLDINGS CANNOT IDENTIFY THE MODEL. The intuitive matcher — compare what they hold —
-- is useless exactly where it is most needed: BUS_FTS_Bepoff_AFS, BUS_FTS_DEF_AFS and
-- BUS_FTS_NEU_AFS hold the IDENTICAL 27 ISINs (27 of 27 of 27 shared), and BUS_FTS_OFF_AFS's
-- 25 are a subset of all three. They are one strategy at four risk weightings. A content match
-- scores all of them 100 and picks whichever it saw first.
--
-- ⚠ AND THE NAME IS NOT A RULE — IT IS FOUR RULES AND A TYPO:
--     AITopSelectie OFF DYN     <-> AITopSelectie OFF FX          suffix swapped
--     BUS_MTS_OFF_AFS_DYN       <-> BUS_MTS_OFF_AFS               suffix appended
--     BUS_FTS_OFF_DYN           <-> BUS_FTS_OFF_AFS               suffix REPLACED
--     BUS_BM_AAN_kw_EUR_2026_d  <-> BUS_BM_AAND_kw_EUR_2026       the word itself mangled
--     VTopSelectie OFF DY       <-> VTopSelectie OFF FX           missing its N
--
-- So the name is the ONLY discriminator that exists, and it is unreliable. That is why this is
-- a stored HUMAN decision and the guess is only ever a hint: a matcher loose enough to pair
-- BUS_FTS_OFF_DYN with BUS_FTS_OFF_AFS is loose enough to pair it with BUS_FTS_NEU_AFS, and
-- the wrong risk profile holds nearly the same names — a wrong link would look right.
--
-- ⚠ ONLY MANUAL CHOICES ARE STORED (same rule as airs_model_portfolio_link). The guess is
-- recomputed on every read, so it cannot rot against a renamed portfolio.
create table if not exists airs_account_model_link (
  id                 bigserial primary key,

  -- The ACCOUNT, by the name AIRS reports it under in airs_performance / airs_holding.
  -- Not an FK: the accounts list is its own scrape and an account can exist with no row in
  -- airs_model_portfolio at all (18 of the 51 do).
  portefeuille       text not null,

  -- NULL is MEANINGFUL: "this account is explicitly NOT running any of our models" — the
  -- benchmarks are exactly this. Without it, clearing a wrong guess would just let the guess
  -- return on the next read, so a user could re-point a link but never dismiss one.
  model_portfolio_id bigint references airs_model_portfolio(id) on delete cascade,

  note               text,
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now()
);

-- One decision per account. Case-folded: AIRS is not consistent (BUS_BepOffensief_Dyn vs
-- BUS_FTS_BEPOFF_DYN in the same list).
create unique index if not exists airs_account_model_link_key
  on airs_account_model_link (lower(portefeuille));

create index if not exists airs_account_model_link_target
  on airs_account_model_link (model_portfolio_id);

grant select, insert, update, delete on airs_account_model_link to anon, authenticated, service_role;
grant usage, select on sequence airs_account_model_link_id_seq to anon, authenticated, service_role;
