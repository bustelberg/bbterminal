# Analyse modal loading profile

Measured locally on 27 October 2026 against the local Supabase database. These are service-call wall times, not production browser paint times. Single before/after runs followed the same navigation sequence; database and process caches affect the results. No production writes, browser tests, builds, or AIRS downloads were used for this profile.

## Measurements

| Surface | Before | After | Notes |
| --- | ---: | ---: | --- |
| Neutraal opening overview | 1.17s | 0.89s | About 3KB; allocation and AIRS/policy scorecard only |
| Neutraal automatic holdings | 3.72s | 1.05s | Risk-column histories deferred; unused model-return reconstruction removed |
| Neutraal Stocks charts | 1.62s | 1.49s | Chart-only request; preserves existing overview |
| Neutraal Bonds | 1.95s / 224KB | No extra request once holdings are loaded | Reuses the complete holdings snapshot |
| Neutraal Alternatives | 1.59s / 224KB | No extra request once holdings are loaded | Same snapshot and in-flight request |
| Neutraal Cash | 1.73s / 224KB | No extra request once holdings are loaded | Same snapshot and in-flight request |
| Toppenberg opening overview | 0.53s | 0.34s | About 3KB; no child transaction ledgers |
| Toppenberg automatic holdings | 6.08s | 2.34s | Includes genuine certificate look-through valuations and results |
| Toppenberg Stocks charts | 1.97s | 1.25s | Chart-only request |
| Neutraal YTD chart | 0.16s | Unchanged data path | About 10KB; chart JavaScript now loaded separately |
| Performance month strip | Previously fetched full selected report | 0.13s / 10.9KB | Position details requested only when a month opens |
| Performance saved month detail | 0.13s / 56.7KB | 0.16s / 56.7KB | Same calculations; cached while the panel remains mounted |

With backend function profiling enabled, the initial Risk tab calls took: Active Share 5.22s, Concentration 0.34s, Correlation 0.36s, Tracking Error 0.77s, Volatility 0.48s, and Drawdown 0.23s. These diagnostic figures include profiler overhead and process/cache warming; they are not a controlled comparison with uninstrumented calls. Correlation used one year of weekly returns, and the other historical views retained their configured periods.

The Active Share response fell from **1,155,712 bytes to 28,890 bytes**. All benchmark constituents still participate in Active Share and overlap arithmetic; only held rows are returned to this table, which already displayed held rows exclusively. Its observed uninstrumented service call after the changes was 1.18s.

Optional Momentum, Volatility and Beta columns now trigger their own full-detail revision only when enabled. The observed Neutraal request with these columns enabled was 2.34s; that cost no longer delays the default table.

## Request and code loading

1. Opening Analyse reads the lightweight overview. It does not load benchmark constituents, holdings ledgers, risk histories, monthly position reports, or optional fund look-through reports.
2. After the overview paints, the holdings request starts automatically, as required by the dashboard. Its key is shared with Bonds, Alternatives, Cash and the unfiltered table. Selecting a sleeve during that request joins it rather than starting another calculation.
3. Holdings default to `include_holding_risk=false`. The saved column selection is respected; enabling Momentum, Volatility or Beta asks for a distinct response with those calculations enabled. Disabled requests never cache empty risk results as real instrument results.
4. Stocks requests only current exposure charts and their supporting holdings metadata. Existing headline returns and allocation remain visible. Benchmark changes and explicit refreshes still rebuild the appropriate overview inputs.
5. Risk itself is dynamically imported. Each of its five secondary tabs is a separate dynamic import and mounts only when selected. Active Share's request is disabled while another Risk tab is selected.
6. The YTD chart, Performance panel and fund allocation modal are separate dynamic imports. Performance initially reads the month strip; selecting a month reads saved detail before considering an AIRS collection job. Completed detail remains cached while this panel is mounted, including the current month.
7. Fund sector look-through requests remain limited to supported held instruments and the Stocks screen. Investment notes and individual fund allocation dialogs fetch when opened. Explicit refresh and month-collection jobs remain user/background actions, rather than work bundled into the first page.

## Correctness and remaining costs

- Holdings cache identities include portfolio, benchmark, source, certificate look-through, refresh/reload sequence and optional risk-column mode. These changes cannot reuse a mismatched snapshot.
- AIRS account return views use AIRS performance and the policy benchmark directly. Model-source views still reconstruct model performance; account views do not need that second calculation.
- Strategy-map lookups now resolve their module path once. The JSON modification timestamp is still checked, preserving live updates to reviewed aliases. Profiling found thousands of repeated Windows path resolutions in certificate-heavy books.
- The remaining Toppenberg holdings cost is mainly resolving the actual linked accounts and loading their valuation/result data. Removing that work would require a separately persisted prepared snapshot, rather than hiding or approximating its results.
- The remaining cold Active Share cost is resolving the workbook constituents and rolling their weights forward using stored EUR prices. Backend caches continue to follow data fingerprints.
- Remote AIRS downloads, uncached fund-provider acquisitions, browser download/parse time and production network latency were not timed. Those paths depend on external systems; the changes keep them off the opening screen rather than claiming they are instant.

## Verification

Regression checks cover shared in-flight holdings, separate data revisions, disabled optional-risk computation, full-benchmark arithmetic with compact Active Share rows, inactive Risk requests, and reading saved monthly detail before starting AIRS. API schemas and frontend types are regenerated for the new optional request parameters.

Final checks: **3,821 backend tests passed, 4 skipped; 1,644 frontend tests passed; TypeScript and ESLint passed.** The first parallel backend run hit a pre-existing stalled-job timing test; that test passed in isolation and the subsequent complete backend run passed without changes to it.

## Persisted preparation follow-up

The two expensive read models are now stored in private `analyse_prepared_snapshot` rows: default AIRS holdings/results for both certificate look-through modes, and ACWI constituent weights rolled forward with EUR prices. The overview reads a compact projection of the saved holdings snapshot, rather than sending its full table before the first paint. Optional instrument-risk columns remain separate live calculations.

One `analyse_prepare` worker runs every five minutes, with at most two portfolios per batch and a 20-second budget checked between calculations. It defers while source refreshes or ingest are active, and holds a PostgreSQL advisory lock so another replica cannot prepare simultaneously. Already-current entries are skipped. Preparation gradually fills the fleet; cache misses and interrupted/failed batches retain the normal live calculation. It is deliberately not injected into multiple refresh chains.

Statement-level source triggers bump durable Holdings/ACWI revisions in the source transaction. AIRS changes invalidate holdings without invalidating unrelated ACWI weights; price/FX/identity changes invalidate both. Saved rows must match source revision, calculation day and calculation code/config version. Revisions are checked before and after reading; a calculation whose sources changed is not saved as current. Python code identity is frozen per process to keep rolling deployments safe, while reviewed configuration and workbook edits are detected live. Anonymous/authenticated database clients have no direct table access.

Fresh-process local reads measured **76ms for Neutraal holdings**, **133ms for Toppenberg holdings**, and **312ms for saved ACWI weights**, without an in-memory warm cache. These remain local service timings, not production browser guarantees. The local migration was applied and source invalidation, permissions and cross-worker exclusion were checked manually without altering portfolio rows. Production requires migration `20261027020000_prepared_analyse_snapshots.sql` and the backend deployment; without them the live fallback remains available.
