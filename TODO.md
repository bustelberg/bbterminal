# Open follow-ups — resume here

Running list of unfinished / offered-but-not-built work, newest context first.
Last updated **2026-06-15**. Delete items as they're done.

---

## 1. Prod data fixes (do soon — local is fixed, prod isn't)

- [ ] **Push the illiquid migration to prod**: `npx supabase db push` →
      `supabase/migrations/20260615005000_company_illiquid.sql` (adds
      `company.illiquid_at`). The /schedule "Mark illiquid" button +
      `POST /api/admin/company-illiquid` + the price-coverage exclusion all need
      the column on prod.
- [ ] **Repoint Bank of New York Mellon ticker `BK → BNY` on prod.** GuruFocus
      renamed the listing (BNY Mellon rebrand). Locally fixed (cid 3327); on prod
      it's still `BK` → every refresh 404s AND the daily delisting sweep will
      **wrongly** mark it `delisted_at` (it's actively trading under BNY). Run the
      same `company.gurufocus_ticker` update against prod.
- [ ] **Re-apply the illiquid marks on prod**: Telecom Italia savings (MIL:TITR)
      via the button once deployed. Covestro (XTER:1COV) is already `delisted_at`
      on prod via the sweep (acquired late 2025) — optionally also mark illiquid.

## 2. Ticker-drift handling (systemic — BK exposed this class)

When GuruFocus renames a ticker, our stored `gurufocus_ticker` 404s, the company
silently stops updating (a 404 loads 0 rows, no error/flag), shows as the
coverage "oldest", and the DB-only delisting sweep can't tell it apart from a
real delisting.

- [ ] **"Fix ticker from GuruFocus" action** — when a company 404s, probe
      GuruFocus by name to find the renamed symbol and repoint
      `gurufocus_ticker` (one click, mirrors the existing "GF name" button →
      `POST /api/admin/gurufocus-company-name`). Surface on /companies and/or the
      /schedule coverage "Oldest" line.
- [ ] **Guard the delisting sweep** (`ingest/delisting.py`) — before stamping
      `delisted_at` on a stale company, probe its GuruFocus symbol; a
      renamed-but-trading listing should be flagged "needs ticker review"
      (`gurufocus_lookup_failed_at`?), not delisted.
- [ ] **Make 404s visible in the price refresh** — a 404 ("Stock not found") for
      a company that HAS prior data currently loads 0 rows silently. Add a
      counter/flag in `ingest/phases/prices.py` so ticker drift isn't invisible.

## 3. Offered UI follow-ups (not built)

- [ ] **/companies "ILLIQUID" badge** — surface `illiquid_at` companies (thread
      it through `/api/companies` incl. the COPY path
      `momentum/data/_pg.py::load_companies_via_copy`, + a badge like
      UNSUBSCRIBED/DUPE).
- [ ] **/schedule "Mark delisted" button** on the coverage **Oldest** line
      (sibling to the new "Mark illiquid" button) — for stale-but-dead listings.
- [ ] **Attribution matrix sector source** (/earnings portfolio mode) — default
      the "Sectors" dropdown to each basket's OWN membership sectors instead of
      always Leonteq (holdings outside Leonteq show "Unclassified").
- [ ] **Mobile-viewport e2e test** (375×667) — assert the nav drawer
      opens/closes + no horizontal overflow. Needs an auth stub so the sidebar
      renders under `E2E_BYPASS_AUTH` (the responsive/drawer work from this
      session is otherwise only structurally verified).

## 4. Data jobs

- [ ] **Re-run the market-cap backfill** — the earlier full run FAILED
      (~73% `market_cap_eur` / 82% `isin` coverage). `uv run python -u -m
      index_universe.backfill_market_cap` (also corrects names) when convenient.
      (The name-only backfill `index_universe.backfill_company_names` completed:
      0 renames — names already correct.)
- [ ] **Re-run the month-end full price refresh after fixing tickers** — the run
      this session showed "0 prices refreshed" partly because stale names like BK
      were 404ing (ticker drift). Re-run to confirm it pulls the long tail once
      tickers are corrected.

## 5. Verify before investing (possibly obsolete)

- [ ] **Earnings basket-aggregate caching** — earlier offer to precompute a
      per-frozen-universe aggregate to speed the ~66s `member-metrics` load.
      Likely **MOOT** after the /earnings single-switch redesign (stock mode =
      stock-vs-stock; portfolio mode = the Allocation×Selection matrix only,
      prices-only). Confirm nothing still triggers the heavy member-metrics
      aggregate before building this.
