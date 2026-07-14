-- A holding that IS another model portfolio.
--
-- Some AIRS positions are not instruments: they are other model portfolios, wrapped as a
-- Leonteq actively-managed certificate so they can be held like a security. "Star Selection
-- Index" (CH1381833321) is held by 11 models and IS `StarTopSelectie OFF FX`. Yahoo has no
-- listing for a structured product, so these rows can never be priced directly — the link is
-- what lets us look through the certificate to the model behind it.
--
-- ⚠ KEYED ON THE HOLDING, NOT ON (PARENT, HOLDING). "Star Selection Index" is the same
-- portfolio no matter which of the 11 models holds it, so the mapping is a property of the
-- INSTRUMENT. Keying it per-parent would mean setting the same fact eleven times and letting
-- the eleven copies disagree.
--
-- ⚠ ONLY MANUAL CHOICES ARE STORED. The educated guess is recomputed on every read (it is a
-- fuzzy match over 95 rows — free), so it can never go stale against a portfolio that was
-- renamed or given a composition. A row here is a HUMAN decision and always wins.
create table if not exists airs_model_portfolio_link (
  id                  bigserial primary key,

  -- The holding, as it appears in a position row. `isin` identifies it when present; a few
  -- rows (cash, in-house lines) have none, so `fonds` is the fallback.
  isin                text,
  fonds               text not null,

  -- NULL is MEANINGFUL: "this holding is explicitly NOT a portfolio". Without it, clearing a
  -- link would just let the auto-guess reappear on the next read, and a user could never
  -- overrule a wrong guess — only re-point it.
  linked_portfolio_id bigint references airs_model_portfolio(id) on delete cascade,

  created_at          timestamptz not null default now(),
  updated_at          timestamptz not null default now()
);

-- One link per holding. ISIN when we have one, else the fund name (case-folded: AIRS is not
-- consistent about it — 'VastgoedTopSelectie index' vs 'MerkenTopSelectie Index').
create unique index if not exists airs_model_portfolio_link_key
  on airs_model_portfolio_link ((coalesce(nullif(isin, ''), lower(fonds))));

create index if not exists airs_model_portfolio_link_target
  on airs_model_portfolio_link (linked_portfolio_id);

grant select, insert, update, delete on airs_model_portfolio_link to anon, authenticated, service_role;
grant usage, select on sequence airs_model_portfolio_link_id_seq to anon, authenticated, service_role;
