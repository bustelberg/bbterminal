# AIRS model portfolios (/management-dashboard)

Insights → Prices checks every saved Leonteq → AIRS Dynamic connection with one
button. It refreshes both source compositions, reads each mapped Yahoo symbol once
per batch, and flags `abs(source_price / yahoo_close - 1) * 100 > 5`. Exact 5%
differences are not alerts. Yahoo requests use the existing shared governor and a
bounded pool; source sessions remain serialized independently.

Checks use completed, unadjusted daily closes from recent history on the source
valuation date. Quote subunits (such as GBp) are normalized. Missing mappings,
prices, dates, incompatible currencies, stale reference dates and source failures
remain explicit, uncheckable results. Cash is excluded. No FX rate or listing is
guessed. Alerts and incomplete coverage remain visible in Prices, and the shared
background popup reports progress and the final counts. An alert is a successful
check; unavailable coverage produces an incomplete job with its partial results
still readable. The job uses the existing `prices` refresh operation and protected
result route; no extra database table is required.

The Portfolios page uses the shared bottom-right job popups for explicit source
refreshes in Portfolios, Connections and Insights. Leonteq lists/compositions,
AIRS compositions and monthly-return comparisons run as background jobs with
real source-stage progress. Quick connection-option reloads use the same popup
stack locally. Saved tables remain visible during refresh; a source fallback
keeps its saved data but produces a failed/incomplete job receipt. Popup Details
retains the recent progress messages.

`POST /api/leonteq/refresh/job` starts a validated source operation; the existing
job transport handles watching, reattachment and cancellation. The admin-only
`GET /api/leonteq/refresh/jobs/{job_id}/result` retrieves its data for 15 minutes.
Portfolio payloads are never emitted on the globally readable job progress stream.

LYNQS reads share one authenticated, in-memory browser on a dedicated owner thread.
Warm refreshes call the page's verified authenticated AMC API directly, with
cache revalidation, rather than navigating the SPA or reusing displayed data.
Only portfolio identifiers and the browser session are reused. API schemas and
composition totals are validated; the portal/CSV reader remains the fallback and
explicitly reloads its page to avoid stale fragment-only navigation.
Credentials changing, browser loss, or a 15-minute session age
recreates it on the next request. Login/browser failures discard it. Optional
mini-charts do not delay a ready portfolio list. Popup progress records the actual
Leonteq source-read duration.

Dynamic connection inspection fetches only the AIRS holdings report using the
existing valued-date fallback and snapshot writer. It does not run the unrelated
performance, mutations, transactions or model-weight reports. Monthly comparisons
fetch AIRS and Leonteq concurrently through their independently serialized sessions,
and validate just the selected connection rather than reloading both catalogues.

Analyse silently preloads Performance's lightweight `monthly-report?month=…&include_details=false`
response after the overview and Holdings paint. Its month tiles and Risk then prepare independently; no positions
or full monthly reports are prefetched. Opening Performance reuses the completed or in-flight
tile response, with a short, generation-aware cache. Clicking a month makes one
`include_details=true` request for its AIRS return, benchmark, positions, contribution
calculations, source dates and reconciliation together. Collapsing or revisiting a complete
cached month adds no request. Explicit refresh/recovery replaces the selected response together.

Monthly benchmark recovery: a month response reports `benchmark_needs_refresh` separately
from missing AIRS position data. The UI never reuses a cached month with a missing applicable
benchmark. It automatically runs `/accounts/{account}/monthly-benchmark/job`, then rereads
the report. Recovery first checks saved data, and only requests the ACWI/AGGH ETF histories
and USD FX if the required return cannot be read. It never redownloads AIRS holdings.
The existing daily post-AIRS job verifies monthly benchmarks for all dashboard accounts,
repairs shared sources once, and retains per-account failures in its durable job receipt.
Missing provider data remains an explicit retryable failure, never a fabricated zero return.

## Prepared Risk tabs

Analyse silently warms the exact current Risk request after its visible full holdings
have painted. Risk's code chunk loads alongside it. The warmup and Risk panel share an
in-flight promise and a parsed, generation-checked cache, so clicking Risk either reads
the completed response immediately or joins the existing preparation. No generic job
popup is opened, including on automatic running-job reattachment. Warmup failures stay
off the Analyse view and are not cached; opening Risk can retry normally.

Risk opens with one `/api/airs/portfolio/risk-bundle` POST for the exact displayed
stock sleeve and policy. It returns Active Share, Correlation, Volatility, Drawdown,
Concentration and the Returns instrument catalogue together. The views share this
response in React context; switching tabs makes no individual risk API requests.
The chart components ship with the Risk panel so switching also avoids lazy chunk waits.
Returns instrument selection/history remains a small, separate saved-series read.

The existing scheduled Analyse preparation writes a complete durable bundle after all
measures finish. The bundle key includes canonical weights, identities, policy and
history start; source revision, calculation day and calculation-code version must match.
Publication checks the source revision again, so mixed generations are never displayed.
If shared sources change during preparation, the worker stops that pass early, waits
for a quiet source window and retries up to three passes. Per-result and shared-history
memory caches include the immediate source revision, rather than relying only on delayed
Postgres statistics. The UI follows a replacement preparation if publication is invalidated
before readback. Continuous refreshes remain bounded and cancellable; no mixed result is
stamped as current. The final read-only poll does not enqueue an unwatched job.
Apply `20261103180000_risk_source_revision.sql` with this change. It adds a dedicated
market/identity source revision, so unrelated AIRS account/report writes cannot invalidate
Risk whose posted weights are unchanged. Market-price, FX, benchmark and identity updates
still invalidate immediately. Before migration, the code safely falls back to the broader
holdings revision and automatic retries; the migration is needed to eliminate unrelated churn.
A cold request returns 202 with one deduplicated background preparation job; it never
runs risk calculations on the HTTP read path. The client follows that job then rereads
the complete bundle. Preparation uses saved prices only, preserving the existing
preliminary daily-volatility fallback and unavailable-history explanations.

## Weekly instrument returns

Analyse → Risk → Returns has a searchable selector across the union of instruments in saved
AIRS history, including sold/closed positions. Its green/red weekly return bars start at the
fixed date 1 January 2026. The year-opening observation is 31 December 2025 (or the most recent
available valuation in that week); the first bar never includes an entire pre-2026 week.

It reads saved weekly `Huidige koers` observations. The normal daily
holdings capture atomically publishes the complete quote set to `airs_weekly_holding_snapshot`,
retaining the latest actual AIRS date per week. This requires
`20261103160000_airs_weekly_holding_snapshots.sql`, which also seeds real historical observations
already stored in `airs_holding`. No extra scheduled AIRS login or vendor price feed is needed.

Load missing history is an explicit cancellable job scoped to the selected instrument.
`airs_weekly_instrument_snapshot` stores only that instrument's observed quote or verified
absence, separately from complete portfolio snapshots. Apply
`20261103170000_airs_instrument_weekly_history.sql` before deploying this reader.
AIRS still supplies dated portfolio reports; only the missing dates needed by the selected
instrument are downloaded. Reports reused for holiday/year-opening fallbacks are fetched once
per job. Other instruments are never saved as a partial portfolio snapshot. Valid saved quotes
and verified non-held dates are reused; explicit retries can recheck genuinely unavailable quotes.
The instrument view uses calendar weeks through Sunday and the last completed Amsterdam calendar
day, so actual Saturday/Sunday AIRS valuations remain usable. The opening observation cannot move
past 31 December 2025, and a weekly fallback cannot leave its own calendar week.

The history loader uses the shared daily-refresh lease and AIRS session lock. It tries dates inside each
requested week, preserves holiday fallback dates, and never modifies live holdings, returns or
transactions. Authentication and download errors remain errors; a genuinely unavailable week stays
missing. The display includes the current week to date and the separate year-opening observation
needed for the first return. Selecting instruments never starts acquisition.

Returns compare the same ISIN and quoted currency in adjacent weekly snapshots. Exact AIRS name
and currency are a labelled fallback when ISIN is absent. Accrued-interest and cash lines are
excluded; conflicting duplicate quotes, non-positive prices and missing weeks produce no return.
Added, sold and re-entered instruments are retained, without fabricated acquisition/exit returns.
Quantity changes do not affect the calculation; income, FX and split adjustments are not included.

## Daily dashboard refresh

`scheduled_jobs.py` declares the clocks and dependency stages. At 06:00 Amsterdam,
`dashboard_market` runs FX, watched-instrument/benchmark-proxy prices, constituent
prices/caps plus momentum, then the daily smart fundamentals check. Fundamentals
prioritise dashboard holdings, deduplicate overlapping indices and fetch only due
feeds within the regional budget. Child stages retain separate durable run records;
they have no independent cron timer.
Fundamentals outcomes use the shared fill's structured receipt: only successfully
loaded companies count as filled. Subscription exclusions are reported as unavailable,
consume no quota reserve and do not fail a run. Vendor failures and interrupted or
quota-deferred work still make the daily pass incomplete; their causes and counts
remain in each scope's receipt.

At 11:00 Amsterdam, `airs_vermogen_refresh` performs one forced dashboard account/model scan.
Manual Refresh all uses the same scope: the five Bustelberg, four Toppenberg, ten
reviewed building blocks, Imagine Fund and two test TopSelecties (22 accounts today).
`management_portfolios.py` shares this definition with the overview's collection map
and existing reviewed JSON allowlists. AIRS discovery and catalogue storage remain
complete; unrelated accounts are filtered before report downloads. A configured account
is never discarded merely because it has fewer than five holdings. Force overrides
freshness, not scope. Progress counts the accounts actually needing downloads.
Account refresh order follows the dashboard: Bustelberg (2.0 Offensief, Offensief,
Beperkt Offensief, Neutraal, Defensief), Toppenberg (Offensief through Defensief),
alphabetical building blocks, Imagine Fund, then alphabetical test TopSelecties.
Already-current accounts are omitted without changing the remaining order. Progress
retains the group and readable portfolio name during each account's report downloads.
Normal Refresh all validates saved price checks against their source fingerprint and
Amsterdam day. Discovery-only roster updates do not invalidate comparisons; changes
to holdings (including child books), prices, mappings, FX or reviewed configuration do.
Current comparisons are skipped. Legacy/missing/outdated comparisons rebuild from stored
prices without fetching Yahoo, sharing source reads across the batch before results are
persisted. Each comparison/save reports progress and cancellation stops before the next
comparison while keeping completed work. Force refresh and individual Force recheck can
still fetch Yahoo. If source freshness cannot be verified, checks rebuild rather than
claiming cached results are current. This optimisation requires no schema migration.
Model composition downloads cover reviewed counterparts, paired models and recursive
manual child links, not the whole catalogue. Price checks, paired-model pricing,
monthly/Analyse preparation, year-end finalisation and freshness use the same dashboard
scope. Generic acquisition helpers retain explicit support for a wider roster.
Model repricing uses today's stored compositions rather than reading AIRS again.
Dynamic/non-Fixed links have no model composition and are recorded as not applicable
for model repricing; their actual instruments remain covered by account price refreshes
and price checks. A Fixed model with no dated composition is not applicable only after
a successful scan today. Failed, unattempted and stale Fixed scans still fail the job.
Pricing receipts retain every model outcome, including reasons for skips and failures.
The overview's `Since` column carries `ytd_from`, the same first-funded-month
anchor as the AIRS return chart, formatted as `1 January 2026`. It uses the year
of the displayed AIRS return and ignores leading unfunded months; it does not claim
an exact intra-month funding date. Migration `20261102010000_airs_overview_return_start.sql`
adds it to the overview RPC. Older RPCs use one batched performance read during rollout.
`dashboard_post_airs` then saves price checks, assembles current-month reports from
stored dated valuations and transactions, prepares Analyse views, and records a
per-portfolio freshness audit. Missing historical month-opening valuations remain
explicitly incomplete; this workflow never backfills them with extra AIRS downloads.
Monthly position returns compare dated AIRS valuations at the previous month-end
and selected month-end, using the existing report date filters. Repeated names
are not automatically ambiguous: same-ISIN/currency lots with compatible unit
marks are combined. AIRS `Opgelopen rente`/`Rente` component labels are preserved
through parsing, daily storage and monthly bundles, so accrued interest is excluded
from the security's price return and reconciled once via AIRS's reported interest
adjustment. Unlabelled conflicting historical rows request a month refresh rather
than guessing the principal from its value. Apply migration
`20261102000000_airs_holding_valuation_component.sql` before deploying these writers.
The parent AIRS receipt retains failed pricing and post-AIRS receipts, including the
nested freshness audit. Logs name the failing model or preparation stage and its cause.

`dashboard_freshness` also runs independently at 15:00 Amsterdam. Its structured
receipt includes actual holding/return/price/FX dates, unmapped instruments, report
completeness, monthly results, preparation availability and upstream job outcomes.
Expand the job on `/schedule` to read these receipts. Partial failures are errors,
not successful zero-row updates. The watchdog cannot initiate extra AIRS passes.
Nested receipts are available for every job and open automatically for failed runs;
the compact summary shows collection counts rather than `[object Object]`.
Watched-price refresh receipts include each attempted ISIN/symbol, previous and
resulting close dates, the market anchor and whether the series remains stale.
An unchanged or partly advanced series is not proof of freshness: unresolved gaps
fail the price job, including startup catch-up (which records the failure without
preventing application startup). A null-price row does not count as a newer close.

Production daily writers require `SUPABASE_DB_URL` or `DATABASE_URL` for a shared
Postgres advisory lock. A durable run-history check limits automatic AIRS attempts
to one per Amsterdam day across replicas. Explicit manual retries remain available.
New in-process queue slices pause while a daily source workflow runs; the morning
workflow waits at most 15 minutes for the previous queue activity to settle.

## Saved dashboard price checks

### ACWI membership source

The newer official iShares CSV snapshot (29 October 2026) takes precedence over the legacy
April XML workbook. Both original exports remain committed. `load_acwi_holdings` selects by
the source date and derives precise equity weights from market values, since the CSV rounds
many genuine small members to 0.00%. Refreshing this committed source requires replacing the
CSV with a newer official export; opening Risk never downloads it.

Active Share and concentration retain every source issuer even when its listing or prices
are unresolved. Such rows are members with unavailable calculated weights, not non-members.
Pricing resolution uses exact symbols first, then exact issuer identity with country/class
checks. Reviewed identity handles the unchanged Everpure security across PSTG/P. The EQR/AVB
successor mapping applies to membership only after 18 August 2026; it never splices old
predecessor prices into Vivmark history. Prepared results include the CSV and identity code
in their version so deployment cannot reuse the old membership answers.

The weight rollforward accepts a single unchanged price mark. A snapshot dated
29 October remains usable when 29 October is also the newest stored close: zero
subsequent price drift preserves its published weight. This exception is specific
to rolling existing weights; ordinary return windows still require two distinct
dates. Mixed baskets retain unchanged names alongside names with newer prices,
then normalise once across both. Price/FX history that is genuinely missing stays
unavailable, rather than being fabricated. Calculation-code versions invalidate
both per-portfolio risk results and the shared ACWI benchmark on deployment.

Risk reads use versioned results in the existing prepared-snapshot table, shared across
workers. The controlled Analyse preparation batch prepares the six default risk views
for each book and certificate scope. Keys include exact equity weights, benchmark policy,
history window, calculation day and deployed calculation code; source changes invalidate
saved results. Aligned histories are reused across views in a bounded memory cache.
Opening Risk never downloads or repairs price history. Missing history stays explicitly
unavailable until source jobs or an explicit repair update the stored inputs.

Analyse opening requests disable optional holding-risk history and bundle allocation, scorecard,
sold-position contributions and both return curves in one response. Curves read saved performance
and benchmark prices without scanning historical holdings or calling vendors. Prepared snapshots
also defer Holdings: after the opening overview paints, one detail response fills the table.
If no prepared snapshot exists, one complete calculation supplies the opening contributions and
is cached for the following Holdings request; this cold opening can take longer.
Hover/focus warms both modal code and
data; reopening can paint a completed client result within its existing 30-second lifetime.
Saved risk-column preferences request their extra history after the holdings mount. Refresh and
selection revisions remain separate cache keys; expired client results are never painted.

`/airs-check` reads persisted native-currency price checks from `airs_price_check` through the
existing `airs_overview_payload` RPC. It has the same user access as `/management-dashboard`
and its own sidebar link. The dashboard no longer displays the price-check column or controls.
Opening checks or viewing their holdings never refreshes Yahoo. Saved results reload every
60 seconds and on window focus; explicit batch/account/instrument actions use the existing
tracked refresh jobs. Group, search and attention filters scope the batch to visible accounts.
The daily AIRS workflow saves checks after account/composition scraping and paired-model Yahoo
pricing; manual account/full refreshes save them too. The check's own freshening step covers held
instruments outside paired models. Failed checks retain the last successful result and timestamp,
and are reported in the refresh job. Accounts without a saved check are labelled explicitly.
Migration `20261020130000_airs_price_check.sql` must be deployed before these writes run.

> Split out of `CLAUDE.md` (2026-08-17) to keep the always-loaded file small. Content is verbatim; `CLAUDE.md` carries a one-line index entry pointing here.

- `/management-dashboard` — **AIRS model portfolios**, a sibling of `/asset-pipeline` in `AssetNav` (`PortfoliosPanel.tsx` → SSE `GET /api/airs/model-portfolios/scan` → `airs_scanner.fetch_model_portfolios_sync`). Stamgegevens › Onderhoud portefeuilles › Model portefeuilles. Traps, all measured 2026-07-13: (1) **the list page TRUNCATES the name** (`BUS_WTS_Dividend...`) and the full value is nowhere on it — not even a `title=`. **The fast path is the list's own "Naar XLS" export** (a *form submit* — `download_via_form`, which since the CRM export was retired on 2026-09-01 exists for this one caller): ONE download gives all 95 full names, and the scan drops from **220s → 6.4s**. The XLS has no `id`, so it's paired with the id-bearing HTML rows **positionally** — a name join is impossible, since `DiTopSelectie OF...` prefix-matches *both* `OFF DYN` and `OFF FX` (19 of 95 are ambiguous) — and **every pairing is then verified against the prefix the list did show**; a row that fails keeps its clipped name and falls back to its edit page (`<input name="Portefeuille" …>`). Also: the export arrives with a **stray leading apostrophe before the zip magic**, which makes pandas reject the entire file ("format cannot be determined") — `_strip_spreadsheet_preamble`. (2) **an out-of-range page does NOT come back empty** — AirSPMS *clamps*, so page 5 of a 4-page list returns 25 rows again and `while rows: page += 1` never terminates; stop on **no new ids**; (3) **AirSPMS serves ISO-8859-1, not utf-8** — decoding as utf-8 silently turned `Azië` into `Azi�` (3 of 95 rows), so `_decode_html` honours the declared charset (falling back to cp1252, the superset servers actually emit). Note the `Portefeuille` code is genuinely capped at **24 chars** by AIRS (`BUS_BM_AAN_kw_USD_2026_d`) — that is the stored value, not our truncation. **Expand a row → its POSITIONS** (`GET /api/airs/model-portfolios/{id}/positions`): the XLS export behind each portfolio (`modelportefeuillefixedList.php?action=xls&Portefeuille=…&Datum=…&type=1` — a real legacy `.xls`, and the link is relative to *that* script, not the edit page) carries **`ISINCode`** — the exact join into `asset_execution` that the AIRS *holdings* sheet never gave us (it has only a fund name like `Alphabet - C` / `L\` Oreal`, which no fuzzy match resolves safely). Each row is flagged `known_instrument`. Two more traps: **the `FixedDatum` dropdown always LEADS with today**, an empty placeholder that returns zero rows — so positions are fetched **newest-first until rows appear**, never `options[0]`; and **pandas' `df.where(pd.notna(df), None)` does NOT stick** on float/mixed columns (it coerces `None` back to `NaN`), so the cash line's missing ISIN arrived as the string `"nan"` — *truthy* — and counted as a holding. `astype(object)` first. A portfolio typed `meervoudig`/`normaal` (the benchmarks) has **no fixed model at all** — an empty table there is an answer, not a failure. **Persisted** (`airs_model_portfolio` + `airs_model_portfolio_position`, migrations `20260713060000`/`070000`/`080000`): the page opens on `GET /api/airs/model-portfolios` (an instant DB read of the `airs_model_portfolio_grid` view); **`/scan` is the explicit refresh** and *writes as it goes*, so a scan that dies halfway leaves behind what it did reach. ⚠ **The scan has a JOB twin — `POST /api/airs/model-portfolios/scan/job` — and that is what the portfolios page's "Refresh all" runs** (2026-08-17). Refresh all is **two operations in sequence**: the account fleet scan (`/api/airs/vermogen/refresh/job`) and then this. Phase two used to be `runSSE` straight into `console.warn`, so for the MINUTES it runs (an edit-page GET + an XLS per ~58 fixed portfolios) the only thing on screen was the button's own label — nothing after a route change or reload, and no way to stop it. Both halves are jobs now: same toast stack, `i/n` off the `count` event's **`i`/`n` FIELDS** (never parsed back out of its message — the scheduler's `_relay` reads the same pair), re-attach via `attachRunningJobs`, and the button's ✕ cancels whichever half is live (`allJob = fleetJob ?? modelsJob`). Cancel stops **between portfolios** (a row's XLS is downloaded, counted and persisted as a unit) via the scanner's optional `should_stop`. ⚠⚠ **And `_emit` now RELAYS to the job's progress line** (`airs_vermogen._PROGRESS` / `_say`, 2026-08-17): `run_airs_vermogen_refresh_sync` called `on_step` only inside the ACCOUNT LOOP, while everything before it — the headless-browser sign-in, three menu navigations, three filters, the paged Front-Office scrape, then the roster + plan reads — narrated exclusively through `_emit` into `_STATUS["log"]`, **which the panel stopped polling when the button became a job**. So the most detailed narration in the module went nowhere and the toast read **"starting…" through the slowest phase of the run**. Both scan jobs now emit a first line *before any work*, naming the sign-in. Two rules: `_emit` forwards the **line only**, never a position (these phases have no denominator, and re-reporting `0/0` from a narration line mid-loop would blank a bar genuinely at 12/44 — `scan_one` narrates four downloads per account), and the sink is **module-level, set inside the `_LOCK` hold and cleared in the same `finally`** (safe because `_LOCK` serialises every writer; left dangling, the scheduler's hookless 05:00 tick would push lines onto the last manual press's finished card). ⚠ **The PER-ROW refresh had the same hole, one level down, and it was ONE MISSING ARGUMENT**: `scan_one` narrates every download as it lands (`on_report`, with per-report timings) and the fleet loop passes it — `refresh_one_portfolio` did not, so a single row emitted 4 lines total and the toast read *"AITopSelectie OFF DYN — scanning AIRS reports"* at 0% for the whole scan (with the cascade, up to 9 accounts × 5 downloads = minutes of one unchanging line). It now uses the same `_say`/`_emit` relay and passes `on_report` for the target **and** each cascaded book — measured 4 → 20 lines, first at +0 ms, bar holding position through each account's downloads. ⚠ `on_report`'s `report` argument is already the human LABEL (`Vermogensoverzicht`), not the `att`/`volk` code — mapping it again prints it unchanged and looks like it worked. Pinned by `tests/test_fleet_progress_relay.py`. ⚠⚠ **And a cancelled worker must RAISE `JobCancelled`, never return a string starting "Cancelled"** — a return is status `done`, and `done` is acted on: the frontend gates phase two on `job.status === 'done'` precisely so a Cancel doesn't buy minutes more of what you asked to stop, so the account job's returned sentence painted a green card *and then started the slow half anyway*. Both jobs now raise. The scan job takes `airs_vermogen._LOCK` **non-blocking** (one AirSPMS session, two threads is corruption) — a **partial** guard, named as such: the scheduler's own model-scan ticks and the SSE endpoint don't take it. Pinned by `tests/test_model_scan_cancel.py`. **Expanding a row is served from the cache too** — `GET …/{id}/positions` reads the DB by default and hits AIRS only on `refresh=true`, on a historical `datum`, or when nothing is stored (measured: **live 14.26s → cached 0.08s, identical rows, 183×**). The cached dict is shaped with **AIRS's own column names** so `_shape_positions` consumes cached and live through ONE code path — they cannot drift. Two rules: **`known_instrument` is never cached** (it joins `asset_execution`, which grows every time you add an instrument, so a stored "not in grid" is wrong the moment the grid catches up), and **a historical `datum` is never written back** (we hold ONE snapshot — the newest — and caching an old one would rot the cache *backwards* while still serving it as current). The response carries `cached_at` and the UI says "cached"; a cached answer shown as fresh is how a stale holding gets trusted. Storing the positions costs **zero extra AIRS traffic** — counting a portfolio already downloads its XLS — and `isin` is the prize. One snapshot per portfolio (newest with rows), **delete-then-insert**, so a position removed from the model actually disappears. **The count is a VIEW, never a column** — a stored integer is a second source of truth that drifts from the rows it counts. **The sortable `Holdings` column** rides the same scan, in a **second SSE phase**: the list still lands in ~6s and renders, then a `count` event per portfolio fills its cell (one edit-page GET + one XLS each — minutes), so the slow part never holds the table hostage. It counts **ISIN-bearing rows only** — cash isn't an instrument — and keeps **three states that look alike strictly apart**: `null` = **no model** (`normaal`/`meervoudig`; AIRS stores no composition, so "0" would be a claim about a model that doesn't exist — these are skipped entirely, ~40% cheaper), `0` = a **real, empty fixed model** (58 are `fixed (…)` but only 57 have a composition — *one* portfolio is genuinely empty, and it is the whole reason `null` and `0` can't merge), and an **error** = we never learned the answer (writing `0` there would fabricate a fact). Both absent states sort to the bottom in **both** directions — absent is not a small number. ⚠ **`fixed (0)` is NOT a holdings count** — 24 portfolios carry it and they hold 20, 9, 1… instruments; the parenthesised figure is the portfolio's own number. **Sortable `YTD (€)` column** (`GET /api/airs/model-portfolios/performance` → `routers/_airs_portfolio_perf.py`) — each model's holdings priced through `asset_execution → asset_price` (Yahoo), EUR-converted per date, weighted by the model's own percentages. **⚠ A MODEL PORTFOLIO IS A COMPOSITION, NOT AN ACCOUNT.** AIRS's snapshot dropdown offers only 2–3 dates (not a monthly history), so **January's composition is unrecoverable** and YTD can only be a buy-and-hold of the *current* one. `positions_datum` **is the model's effective date** (its inception), and it decides where the YTD can honestly start — so **the YTD anchor is `max(Jan 1, inception)`, never an unconditional Jan 1** (`ytd_anchor`, returned as `ytd_from`). Effective **before** Jan 1 (**29 of 56**) → the model held these weights all year → a real, full YTD. (Re)defined **during** the year (**27 of 56**) → the window opens at the inception, so the figure is a **PARTIAL year: realized, not backtested** — and for those rows `ytd_pct == since_model_pct` by construction. Pricing them back to Jan 1 instead would **backtest a basket chosen with hindsight**, which is what it did until 2026-07-14: **MoTopSelectie_FX read +75.85% YTD on a model defined 8 days earlier** — the best portfolio in the list, on weights it had never held; its realized return over those 8 days was **+0.51%**. `model_changed_in_period` (⚠ in the UI) survives the fix with a **new meaning — "partial year, don't rank it against a full one"**, not "this is a backtest". **⚠ `GBp` IS PENCE, NOT A CURRENCY CODE** (2026-07-14) — Yahoo quotes every London listing in pence, so `asset_execution.currency` reads `GBp` for **343 rows**, and `fx_rate` has **no such row** (it has `GBP`). An FX lookup that passes the literal string through gets nothing back, and every caller reads a missing rate as *"this holding cannot be priced"* — so Judges Scientific (`GB0032398678`, **5,930 bars back to 2003**) read as unpriced in all **13** AIRS portfolios holding a pence-quoted name, with every bar sitting in the table. **The failure is silent in BOTH directions**: forget to normalise the *code* and the holding disappears; forget the *divisor* and a £46.75 share prices at **£4,675** — a hundredfold error that still looks like a number. So `routers/_benchmark_index.py::_rate` resolves the minor unit through **`asset_pipeline.fx.SUBUNIT`** (shared, never re-derived) and scales the RATE by the divisor rather than leaving each caller to divide the price (`eur = pence / (100 × gbp_rate)` — one place, impossible to half-apply). **Both FX loaders must ask for the MAJOR currency too** (`_airs_portfolio_perf._fx`, `_benchmark_index._fx_to_eur`): fixing `_rate` alone leaves the dict with no `GBP` key and the holding just as unpriced. `ZAc`/`ILA` are in the same map. Pinned by `tests/test_minor_unit_fx.py`. **⚠⚠ AND BOTH FX LOADERS MUST *PAGE* — AN UNPAGED FX READ IS THE ONE TRUNCATION THAT PRODUCES A NUMBER INSTEAD OF A BLANK** (2026-08-03). They read every held currency's whole history in ONE PostgREST request — **19,037 rows over 27 currencies** — and PostgREST truncates **silently** at 1,000 rows on cloud / 10,000 locally. A missing rate is invisible twice: `_eur_series` drops any close with no rate on or before it, so a currency cut at the **head** has no EUR series before the cut; the holding then has no mark at the anchor, is classed *unpriceable*, **silently leaves the basket**, and the return renormalises over what survived. Measured: TWD came back as **20 rows starting 2026-05-27** (real history 2014), so **Taiwan Semiconductor — 5% of `AITopSelectie OFF FX`, 6,606 bars, correctly resolved — vanished from its own portfolio's return**; a cut *tail* is quieter but also wrong (it freezes the rate at an old date, so every recent EUR conversion drifts). Because the two caps differ tenfold, **each environment cut different currencies and reported a different YTD off identical code**: 36.64% local vs **44.14% production** for that model, and **49 of 56 portfolios changed** once paged. No empty cell anywhere — a list truncation shows up as a missing row, this one shows up as a *plausible number*. Two rules beyond "page": **advance by what came back** (`off += len(rows)`, break on an EMPTY page — `len(rows) < page` is only correct while the server's cap is ≥ the page size, which is the assumption that failed), and **sort on a UNIQUE key** (`.order("rate_date").order("currency_code")` — Postgres makes no promise about tied rows across separate LIMIT/OFFSET queries, so a page boundary inside a tie serves a row twice or never; `_closes_paged` had the same tie). Pinned by `tests/test_fx_paging.py`, against `tests/_fake_supabase.py`'s new `max_rows=` (it truncates silently, exactly as PostgREST does — use it to prove any reader pages). **Diagnose a per-model disagreement with `GET /api/airs/model-portfolios/{id}/ytd-explain`** (`explain_portfolio_ytd`, + the expanded row's "Explain YTD → console" button): it *instruments* `compute_portfolio_performance` rather than recomputing — so the figure it explains IS the grid's — and returns three levels ordered by how far upstream a cause sits: `load` (price transport, FX/price windows, freshest close, per-currency row counts and date spans — where this bug is visible in one glance), `portfolio` (anchor, coverage, priced weight), `legs` (per holding: mark, end close, EUR return, contribution in pp, summing to the YTD exactly — `reconciles`). **A coverage floor is load-bearing**: 25 of 248 held ISINs have no price series (Leonteq structured products, in-house funds — the zero-bar guard refuses to map them), and renormalising over the rest at 95% is a rounding error but **at 1% is a fabrication — TOPS_OFF_BEH once read "+0.00%", which was its 1% cash line alone** while nine structured products were silently dropped. Under `MIN_COVERAGE_PCT` (60) the API returns **no number** and `covered_pct` as the reason; 16 of 56 are refused. **Cash is priced at 0%, never skipped** — its drag is a fact (dropping it scales a 20%-cash portfolio's return up by 25%). Same `_split_adjust` + `_at_or_before` as the benchmark, and the price read **must page** (223 holdings × ~130 days ≈ 29k rows vs PostgREST's silent 1,000-row cap — this bit me in a probe, which reported 102 priced holdings when the answer was 221; it now prefers ONE `COPY` via `timeseries.load_series("yf.close")` and falls back to the pager, because the since-inception window reaches back to 2024-06 and the read is ~118k rows). **Sortable `Since incep. (€)` · `Sharpe` · `Sortino` columns** — all three ride **ONE window**, from the composition's own effective date, and are read off **ONE daily EUR buy-and-hold curve** (`_index`, whose end value IS the weighted point-to-point return, so `since_model_pct` is not computed a second way). A ratio is only as honest as the return under it, and a YTD-anchored one is a backtest for half the list — so the ratios are **never** YTD-anchored. `annualized_stats(…, periods_per_year=252)` (shared with the monthly diversifier — the cadence is a parameter, not a fork), rf = 0. Traps: (1) **⚠ AIRS's `fixed_datum` is NOT the composition's date** — they disagree on **39 of 56**, and `fixed_datum` is **EARLIER on 33** of them (`BUS_FTS_OFF_AFS`: list says 2025-03-06, the model we hold is dated 2025-04-11). Anchoring there would price the weights **five weeks before they were chosen** — the hindsight bug this module exists to refuse. The anchor is `positions_datum`; where the two differ the Fixed-date cell shows **both** (`model <date>` underneath), because three columns measured from a date the table doesn't show is an invitation to check a return against a window it was never computed over. (2) **Ratios are withheld under `MIN_STAT_DAYS` (20) daily returns** — 27 of 56 models were (re)defined this year, one **8 days** before it was measured, and a Sharpe off 5 points renders in the same column, same font, as one measured over two years. (3) **The since-inception window carries its OWN coverage floor** (`since_covered_pct`) — a holding that had not listed yet at a 2024 inception is unpriceable *there* whatever `covered_pct` says at Jan 1, and `_index` renormalises over what is left, so the weight it could actually hold has to come back out and be gated. (4) Holdings sit on **different exchange calendars** (a Tokyo holiday is a normal Wednesday in Paris), so the curve samples the **union** of trading dates and forward-fills: a missing bar is "still held, last price", not a 0% day and not a hole — read either other way, the daily vol under Sharpe is fiction. **`Link` column (expanded row, between Fund and Weight)** — **some holdings are not instruments, they are other model portfolios** (`routers/_airs_portfolio_links.py`, migration `20260714000000`, 2026-07-14), wrapped as a **Leonteq AMC certificate** so they can be held like a security: `CH1381833321` "Star Selection Index" **IS** `StarTopSelectie OFF FX` and is held by 11 models. Ten such certificates exist; all are CH ISINs Yahoo can never price (the zero-bar guard is right to refuse them), so they sit in the table as dead rows whose weight comes out of the coverage denominator — the link is what lets us look **through** to the model behind them. Per-row dropdown (`GET …/{id}/linkable` — ONE call per expand, not one per row) + an **auto-guess with a confidence badge**. **⚠ THE LINK IS KEYED ON THE HOLDING, NOT ON (PARENT, HOLDING)** — one fact, stored once; keyed per-parent it would be set 11 times and the copies would disagree. **Only MANUAL choices are stored**; the guess is recomputed every read (a fuzzy match over 95 rows — free), so it cannot rot against a renamed portfolio. A stored **NULL is meaningful** ("explicitly not a portfolio"), or a wrong guess could only be re-pointed, never dismissed. Three traps, each of which alone yields a **confident wrong link** — the failure that matters, because nobody re-checks a 99%:
  - **⚠ THE BEST NAME MATCH IS THE WRAPPER, AND IT IS A CYCLE.** `TOPS_STS_L`'s description is literally "StarTopSelectie" — the closest string to "Star Selection Index" in all 95 — and it **holds the certificate at 100%**. Following that link walks back to the row you started from. The strategy it actually tracks, `StarTopSelectie OFF FX` (24 real stocks), scores **worse**. So the gates are not tie-breakers on a name score, they are **what makes the name score usable**: no self-reference, **no portfolio that HOLDS this holding**, and the target must have a composition >1 (the `OFF DYN` twins store none — a link to nothing).
  - **⚠ `token_set_ratio` SCORES A SUBSET AS 100.** stem(`BUS_EUR_OFF_FX`) = `eur` ⊂ stem(`Shell PLC EUR`), so **every EUR-quoted holding** (Shell, iShares ACWI, Vanguard Japan, Amundi EM Asia…) linked to the Europa portfolio at 0.89 — 20 false links. Use **`token_sort_ratio`**; same false-friend family as the raw `_name_score` floor that put NVIDIA on Stuttgart.
  - **⚠ THE STEMMER DELETES THE WORD THAT SEPARATES TWO PRODUCT LINES.** "TopSelectie" must come off (every strategy carries it) — but then `EuropaTopSelectie Index` and `Europa Offensief FX` **both stem to `europa`**, and the latter (27 positions) passes every gate at a perfect 100. Fixed by asking `is_topselectie()` **before** stripping (gate 4: same product line). The genuine `EuropaTopSelect OFF FX` stores **no composition**, so the honest output there is **no guess** — a module that cannot say "I don't know" says something false instead. Noise is removed as a **SUBSTRING, not a token** ("Familietopselectie" is one word — a tokenizer removes nothing).
  Measured: Star/AI/Azie/Momentum/Dividend → **0.99**; Vastgoed **0.80** (AIRS's own typo "VastgoeddTopSelectie"); Familie/Merken → **0.60 ⚠ ambiguous** (four risk-profile variants — the names cannot say which; a top score that barely beat its runner-up is a coin flip, not a 0.99); Europa, "High Income Quality fund", and every real instrument → **no guess**. Pinned by `tests/test_airs_portfolio_links.py`.

  **Leftmost `Analyse` button → the composition modal** (`portfolios/PortfolioAnalysisModal.tsx` → `GET /api/airs/model-portfolios/{id}/analysis` → `routers/_airs_portfolio_analysis.py`): grouped horizontal bars of the model's **sector / region / currency** beside the benchmark index (**ACWI** by default since 2026-08-18 - these books are global and multi-currency, so an all-country index is what they are actually managed against; `SP500` and `AEX` stay in the picker), with the tilt (pp) in the tooltip. Three traps, each of which alone makes the chart a lie:
  - **Benchmark picker: `SP500` | `ACWI`, both rebuilt in the ASSET world** (`routers/_asset_benchmark.py`, 2026-07-14) — membership joined by **ISIN** (`universe_membership.company_id → company.isin → asset_execution.isin`), prices from **`asset_price` (yfinance)**, weights from `asset_grid.market_cap_eur`. **⚠ THE BENCHMARK MUST BE PRICED IN THE SAME WORLD AS THE PORTFOLIO** — the portfolio's return comes from `asset_price`; pricing the index off GuruFocus (`_benchmark_index`, which `/benchmarks` still uses to validate against SPY) would compare two price universes with different adjustment conventions and different FX, and call the difference alpha. **⚠ AND GURUFOCUS CANNOT PRICE ACWI AT ALL**: its subscription has structural holes (no UK, India, Ireland, AU/NZ, Africa, LatAm) — **LSE 72 ACWI members → GuruFocus prices 0; NSE 160 → 0** — worth **~7.8% of ACWI's published weight**, and a cap-weighted index renormalised over the other 92% does not *lose* that weight, it **redistributes** it. yfinance has no such holes. **The bridge is a JOIN, never a COLUMN** — a membership flag on `asset_execution` would need re-syncing on every ACWI reconstruction and would be silently wrong the day it drifts (same rule as *"the count is a VIEW, never a column"*). **The weighting is REUSED, not copied**: `_benchmark_index._window_rows` (start-of-window cap weights — weighting by today's cap is the look-ahead bias that turned +9.10% into +21.70%; a second copy is a second place for it to grow back). Coverage is **always reported** (`benchmark_coverage_pct`) and warned on in the UI below 97%, because ACWI's missing names go **a whole country at a time**. Measured 2026-07-14 after ingesting 305 missing constituents (`scripts/ingest_universe_assets.py --universe ACWI`) + a cap backfill: **SP500 490/493 (100%), ACWI 1,598/1,998 (84%)** — China A **0→148**, Korea **2→57**, UK **0→44**, all of which GuruFocus prices zero of. ⚠ **India (160 members) is still missing** — those `company` rows carry **no ISIN**, and GuruFocus cannot supply one (it is blind to India), so the bridge cannot be built; needs a non-GuruFocus identifier route. ⚠ **Still a PRICE return and still FULL-cap** (MSCI float-adjusts): validated against the real ETF `ISAC.L` (`IE00B6R52259`, already in the grid, 3,708 bars) — ETF **+14.01%** EUR vs our **+10.81%**, of which **~1.1pp is dividends** (ISAC is an **accumulating** share class, so its price IS a total return — unlike SPY, which is distributing and made the S&P cross-check clean) and the rest is the coverage hole. Pinned by `tests/test_asset_benchmark.py`.
  - **Click a return row → `Why —` : Brinson-Fachler attribution** (`routers/_airs_portfolio_attribution.py` + `portfolios/AttributionPanel.tsx`, `GET …/{id}/attribution?benchmark=&window=ytd|since&axis=sector|region|currency`). An excess return is a **fact, not an explanation**: "−15.34% vs ACWI" says nothing about whether the failed bet was the **sectors** chosen or the **stocks** chosen inside them — different mistakes, different fixes. `allocation = (w_p − w_b)(R_b,bucket − **R_b_total**)` · `selection = w_b(R_p,bucket − R_b,bucket)` · `interaction` = the cross term. ⚠ **The `− R_b_total` is the Fachler part and it FLIPS THE SIGN of real calls**: overweighting a sector that rose 5% while the *index* rose 10% is a **bad** allocation decision, and plain Brinson scores it **positive**. ⚠ **THE IDENTITY IS ASSERTED, NOT ASSUMED** — `sum(allocation+selection+interaction) == excess` (`residual_pct`, `reconciles`; measured residual ~1e-15). Three columns that don't sum to the excess are not a decomposition of it, and the panel refuses to present them as one. **Funds and cash are EXCLUDED** (an ETF has no sector; the benchmark's weight in the fund bucket is zero, so Brinson would report holding a world tracker as a *sector bet*) and `attributable_pct` states how much of the model the table explains. ⚠ **`unpriced_pct` IS A DIFFERENT EXCLUSION AND IT IS THE DANGEROUS ONE**: an unpriceable *equity* is dropped too, but its sector then reads as **UNOWNED** — measured, a model holding **6% Healthcare** was credited **+1.73pp of allocation for "avoiding" Healthcare**, a sector it actually owns. A false finding, not a missing one; `unpriced_buckets` names the rows to discount. **Contributors / detractors / "index winners you did NOT own"** ride along — ⚠ the last matched by **COMPANY, not ISIN** (`same_company`): Alphabet is **GOOGL** in the index and **"Alphabet - C"** in the model, two ISINs and one business, and ISIN-matching reported it as a **+3.23pp missed winner while it was the portfolio's single largest contributor (+5.92pp)**. Also fixed here: ⚠ **Yahoo speaks TWO sector vocabularies** (`Materials`/`Basic Materials`, `Financials`/`Financial Services`) — unmerged they are two BUCKETS, so Brinson reads a portfolio in "Financial Services" against an index in "Financials" as a **phantom allocation bet with zero selection**; canonicalised in `_airs_portfolio_analysis._sector`. Pinned by `tests/test_airs_portfolio_attribution.py`.
  - **`Return (€)` tile** — the model's EUR return vs the index, over **two windows (YTD + since inception)**, with the excess. **⚠ A BENCHMARK MEASURED OVER A DIFFERENT WINDOW IS NOT A BENCHMARK, IT IS A NUMBER.** A model's YTD opens at `max(1 Jan, inception)` and **27 of 56 are younger than the year** — `MoTopSelectie_FX` is **nine days old**. Setting its −3.04% beside the index's full-year **+12.41%** and calling the gap under-performance is nonsense that reads exactly like a finding; over those same nine days the S&P made **+0.27%**. So `_benchmark_index.index_returns(label, starts)` prices the index from the model's **own** `ytd_from` *and* its **own** inception — both off **ONE price load**, through the **same `_window_rows`** the `/benchmarks` page uses (two `compute_index` calls would reload every close *and* give the start-of-window weighting a second place to rot; `compute_index` now takes an optional `start`, default unchanged). `ytd_is_since` is surfaced so a young model's two identical rows read as *by construction*, not as a bug. **The portfolio side is READ from `compute_portfolio_performance`, never recomputed** — re-deriving it "the same way" is how a modal quietly disagrees with the row that opened it.
  - **⚠ ONE VOCABULARY, OR THE COMPARISON INVENTS A TILT.** The portfolio lives in the ISIN world (`asset_execution`); the benchmark in the `company` world — and **`company` has no sector column at all** (it would come from `universe_membership`, whose SP500 rows are **empty**). "Technology" vs "Information Technology" is not a bet. So **both sides are classified from `asset_grid` (yfinance), joined by ISIN** — measured: all 493 SP500 members are there with a sector, so the benchmark loses nothing.
  - **⚠ FUNDS ARE NOT LOOKED THROUGH, AND THE CHART SAYS SO.** An ETF's *listing* tells you nothing about its contents: **24 of the 26 held ETFs have a "sector" of literally `etf` or `Equity`**; an Amsterdam-listed MSCI World ETF is not European exposure; quoted in EUR it still holds mostly USD assets. Counting those would push ~20% of a portfolio into a phantom sector and deflate every real one — so a fund goes into ONE **`Fund (not looked through)`** bucket on **all three** axes. A 40%-ETF portfolio shows a 40% bar meaning *"we cannot see inside this"*, which is true and beats a confident invented split. Cash is a bucket too (its drag is a fact); an unclassifiable holding is `Unclassified`, never dropped.
  - **⚠ REGION IS THE ISSUER'S, NOT OUR VENUE'S — THE S&P 500 IS NOT 7% EUROPEAN.** `asset_grid.msci_region` comes from `geo.resolve_geo`, which **falls back to the LISTING country when the domicile is unknown** — sane for the instrument grid, catastrophic here. Yahoo returns no domicile for a thin German regional line, and our grid prices US megacaps on exactly those: **`LLY.SG` Eli Lilly on Stuttgart (€873bn), `CHV.DU` Chevron on Düsseldorf, `IBM.HM` IBM on Hamburg** — all inherited "Germany" → **Europe**. 54 members were classified Europe (**7.2%** of cap). `_region()` now goes **domicile → the ISIN's own country prefix → `Unclassified`, never the listing**: 20 members, **2.1%**, and those are *real* (Linde, Eaton, Accenture, Medtronic, Trane, Johnson Controls = Irish; Chubb = Swiss; NXP = Dutch — the S&P genuinely holds foreign-domiciled constituents). **The job is to lose the 5.1%, not to force the index to 100% American.** Known limit: an ADR carries a US ISIN, so a *domicile-less* ADR reads North America — it only bites when Yahoo gave no domicile at all, which for an ADR it usually does.
  - **⚠ CURRENCY IS THE COMPANY'S, NOT OUR VENUE'S.** By *listing* currency the S&P 500 reads **91% USD**; by `market_cap_currency`, **98%**. The gap is **40 of 491 members our grid prices on European/Canadian venues** (Corning on Stuttgart, Ciena on Xetra, WR Berkley on Munich, Exxon on a Canadian line) — the wrong-listing bug, **inside the benchmark**. Pricing Corning off Stuttgart does not make it a euro asset, and the naive chart called the S&P *"12% EUR"*. Fixed by reading the reporting currency, and the mismatch is **counted and surfaced** (`benchmark_foreign_listings`) rather than absorbed — it also means **the benchmark's PRICE SERIES is drawn off those venues**. Benchmark weights come from `_benchmark_index._members` (already deduped — GOOGL+GOOG would double-count Alphabet's cap, 11.3% of the index, fictional); they are never re-derived.

  **⚠⚠ ONE REFRESH FUNCTION, BOTH HALVES — `routers/_airs_full_refresh.py::refresh_portfolio_fully` (2026-08-18).** A portfolio is a PAIR in AIRS (the Fixed model = weights + ISINs + an effective date, and the Dynamic book = real positions and money) and **each refresh button used to refresh ONE half**: /management-dashboard's row ran `airs_vermogen.refresh_one_portfolio` (the book's Rendement + Vermogensoverzicht, cascaded through certificates), /portfolios' row ran `_airs_portfolio_refresh.refresh_portfolio` (composition → instruments → FX → prices → recompute), and the **Analyse modal offers one "Refresh" and inherited whichever panel opened it** — so the same-looking button did different work depending on where the reader came from, and no button on /management-dashboard could refresh a model composition at all. ⚠ **Neither half's docstring was wrong, which is why it survived**: each described its own scope accurately (`refresh_portfolio`'s header even says in capitals that "REFRESH FROM AIRS" cannot fix a wrong return on its own). There was simply no object called "a portfolio refresh" for either to be half of. Takes EITHER handle (`portefeuille` or `portfolio_id`) and resolves the other through `list_account_links` — the pairing rule that already exists, never a second one. ⚠ **The AIRS legs are serialized and NOTHING ELSE IS** (`airs_vermogen._acquire_session`): AirSPMS is ONE authenticated session that two threads cannot drive — they interleave into each other's downloads, which is not an error — so the report downloads and the composition read take `_LOCK` while the OpenFIGI resolve, the ECB/Yahoo FX backfill, the price fetch per holding and the recompute run outside it. That split is what makes `refresh_many`'s fan-out **faster** than N sequential calls rather than merely safe. ⚠ It also **closed a gap `routers/airs.py` documented as known and accepted** — `_composition` took no lock at all, survivable only while exactly one human pressed one button. ⚠ **`wait=None` refuses, a number queues**: a BUTTON must answer ("another AIRS refresh is running" is true and actionable), but a leg of a job already under way must not abandon a portfolio half-refreshed. ⚠ **Both halves run even if one fails** — they read different sources, so a Yahoo outage must not leave the AIRS half stale — and the verdict is the **worse** of the two; an absent half (18 of 51 accounts have no model) is reported as `absent`, never counted as done. ⚠ **Two hooks, because the two callers ask different questions**: `on_step` is one bar across both halves (each owned the bar before, so a full refresh ran 0→100% twice and read as the job restarting), `on_event` relays each half's own frames so the /portfolios console still bolds the five `phase` lines — flattening them onto `on_step` was tried first and cost exactly that, with no error anywhere. Pinned by `tests/test_airs_full_refresh.py`. ⚠ **Still NOT converted: `run_airs_vermogen_refresh_sync`** ("Refresh all" + the scheduler tick) holds `_LOCK` for the WHOLE run, so calling the per-leg-locking function from inside it self-deadlocks — see [`TODO.md`](../TODO.md) for the three invariants a conversion lands on.

  **⚠ THE MODAL'S FRESHNESS BADGES NEED TWO DATES, AND CARRIED ONE (2026-08-18).** `Provenance` colours its ⓘ from `as_of` (when AIRS *valued* the book) paired with `fetched_at` (when *we* last read it) — only the combination says whose lag a stale date is, and `lagOwner` returns **null rather than guessing** when the second is absent, which the badge reads as "cannot rule out that it is ours" and paints amber. The analysis payload carried no `fetched_at` at all, so **every badge in the modal warned on a book the row called current**, from the same two facts — and **no refresh could clear it**, because a refresh updates exactly the fact that was never sent, which made the button read as broken. Fixed with `holdings_fetched_at` (from `_airs_accounts._fetched_at`, the SAME loader the row uses — two readers of one column is how they would drift apart again) + `ProvenanceFetchedAt` wrapping the modal's subtree. ⚠ **And the nested Fundamental / Holding-timing modals reset it** (`at={undefined}`): they render inside that box for event-propagation reasons only and describe a COMPANY, so inheriting the book's fetch time would de-amber a staleness that really is ours. ⚠ Note `as_of` (the *model composition's* effective date) is legitimately old — AITopSelectie's is **2025-12-30** and AIRS has published nothing newer; the newest date its edit page offers is always **today**, whose XLS is empty, so `fetch_portfolio_positions_sync` correctly falls back. **64 of 102 models sit on a composition older than the newest date AIRS lists, and that is not our staleness.** Pinned by `tests/test_airs_analysis_fetched_at.py` + `lib/lagOwner.test.ts`.

  **Palette: computed, not eyeballed.** The app's standard A/B pair (accent blue + `compare` violet) **FAILS** colourblind separation — ΔE **4.9** under deuteranopia, i.e. one colour. Blue + `warn` amber scores **103**. Run `dataviz/scripts/validate_palette.js`, don't reason about it. The amber sits a hair under 3:1 contrast on white, which obliges relief, so every bar carries a **direct value label** (in ink — text wears text tokens, never the series colour). Pinned by `tests/test_airs_portfolio_analysis.py`. **Expanding a row adds the ARITHMETIC BEHIND its YTD** — five per-holding columns on the positions table (`Start (€)` · `Start date` · `End (€)` · `End date` · `Return (€)`, from `compute_holding_marks`, attached in `_shape_positions` and **never cached** — a price mark is true for one day, the composition it prices is not). Each holding is bought at its last close on or before the window and held to its latest close; **weighting the `Return (€)` column by the model's percentages reproduces the row's YTD exactly** (measured: AITopSelectie OFF FX, 51.4812% both ways), because both read the same EUR series through the same anchor — `ytd_anchor_for()` is the **ONE** definition of that anchor, shared by both, so the entry prices printed under a +51.48% cannot belong to a different window than the number they explain. ⚠ **The prices are in EUR, not the listing's currency** — `return_pct` is an EUR return and carries the FX leg, so printing the native closes as the arithmetic would show two numbers whose ratio is not the third (a USD holding rises in dollars and falls in euros on the same days); the native close + currency ride along as the **tooltip** (`*_price_local`), never as the sum. **End dates differ per holding** — vendors publish unevenly, so a lagging name is marked at its last known close and the date says so. **⚠ A SPARSE SERIES GETS AN INTERPOLATED OPENING MARK, AND IT SAYS SO** (`_mark_at`): a close within `_MARK_GAP_TOLERANCE_DAYS` (7) before the anchor **IS** the anchor's price (31 Dec is the 1 Jan mark — interpolating there would replace every observed opening price with a modelled one), but when the series has no price near the date and real closes **bracket** it, the opening value is **linearly interpolated** between them and flagged (`start_interpolated`, `start_gap_days`; ⚠ + `(est)` in the row, `≈` on the portfolio's YTD, `interpolated_holdings` in the payload). Beyond `_MAX_INTERP_SPAN_DAYS` (400) it is **refused** — straight-lining across more than a year is invention, not estimation (same refusal as `_trailing_12m`'s 450-day cap) — and a series that *ends* before the window is never **extrapolated** forward. `_index` and `compute_holding_marks` both mark through `_mark_at`, so the itemised rows still weight to the portfolio figure exactly. ⚠ **The load window is a PERFORMANCE bound, never a correctness one** — `_prepend_opening_bars` fetches the opening bar however far back it sits, and the FX window is then widened to reach it (`fx_from`), because a bar with no rate on or before it is silently dropped by `_eur_series`. It *was* a correctness bound until 2026-07-14: iShares Euro HY (`IE00B66F4759`) is mapped to **`ISHHF`, a US OTC line with 54 bars in TEN years**, whose last close before 1 Jan was 2025-11-03 — 14 days outside the expanded row's 45-day lookback. The row showed a blank while the portfolio figure, loading from the earliest inception, priced the same holding at **23.6% weight**. Two windows, two answers, no error. **The interpolation is a mitigation, not the fix — that ETF is on the wrong listing** (`scripts/repoint_etf_listing.py`, by ISIN). No marks at all for cash (no ISIN, no series — it is priced at a flat 0% *inside* the portfolio figure), an unresolved ETF, or a holding that had not listed when the window opened; a `0%` there would be a claim, not a blank. **Sortable `Resolved` column** — how many of the model's instruments we can actually price, i.e. have a **Yahoo (`asset_price`) series** for. It reconciles **exactly** with `Holdings` (`resolved_holdings + unresolved_holdings == holdings`): both count **DISTINCT ISINs** (a model may list one instrument twice — VTopSelectie OFF FX holds CapitaLand at 2% *and* 3%, so counting rows would report 29 resolved of 28 held) and both **exclude cash** (no ISIN, not an instrument — `priced_holdings` is the cash-inclusive leg count and is a *different* number). Shown `n/m` in amber whenever short, because the gap IS the weight the returns are renormalised over — and it is the **visible cause of an `n/a` row**: BUS_Alternatives_FX holds 9, prices 5, and its missing 45% is three ETFs still `queued` in the grid (Global X SuperDividend, YieldMax, WisdomTree — fixable with `scripts/asset_resolve_missing.py`) plus an in-house fund with no listing at all (permanently unpriceable), so coverage is 55%, under the floor, and every figure is refused. Absences never look like zeros: `n/a` = unpriceable, `—` = window too short / denominator undefined (a curve that never fell has no downside deviation — Sortino is *undefined*, not infinite), and a **no-model** row shows `—`, not the `…` a loading row wears. **"Hide small portfolios"** (checked by default) is a **KEEP rule** — it shows only the rows with a counted model holding **> 5** instruments (42 of 95), so the single-instrument wrappers *and* every row with no countable model (no fixed model, no snapshot, count failed) are all hidden together; unchecking restores all 95. Pinned by `tests/test_airs_portfolio_perf.py`. **Instruments** is the flat per-ISIN grid (`asset_grid` view, TanStack virtualized, all rows client-filtered). The three rightmost columns — **Exchange · Ticker · Div/share** (`GURUFOCUS` source badge) — are the only ones not sourced from Yahoo/OpenFIGI/Leonteq. **TWO bridges reach GuruFocus** (`routers/_asset_dividends.py`, the ONLY place the two entity universes meet), and which one a row takes decides what data it can have:


---

**Continues in** [`gurufocus-data.md`](gurufocus-data.md): the two GuruFocus bridges behind the Instruments grid (Exchange · Ticker · Div/share), `pick_listing`, the US exchange-code minefield, the dividend/financials states, and the rebuilt benchmark index.

## What the Analyse modal costs, and where it goes (2026-08-19)

The modal is **one** request (`GET /api/airs/model-portfolios/{id}/analysis`, or
`POST /api/airs/basket/analysis` for a book with no paired model). It has no partial paint: nothing
is on screen until the whole payload lands, so its wall clock *is* the user's wait.

**Measure before touching it — `backend/scripts/profile_analysis_modal.py`.** It wraps both
transports (PostgREST over HTTP, and direct-Postgres `COPY`) plus the loaders the endpoint
composes, and prints per-phase ms, per-table round trips, byte-identical repeats, the tables read
through several query shapes, and the leg-cache hit rate. `--id a,b,c` runs several books in one
process, which is the realistic measurement; `--cold` drops the cross-portfolio cache before every
run, so a before/after is one flag apart.

⚠⚠ **REQUEST COUNT IS THE PRODUCTION COST; LOCAL WALL TIME IS NOT.** Local Postgres answers in
single-digit ms, production is eu-west-3 with the backend elsewhere at ~60ms a round trip. A phase
that is 200ms locally over 30 requests is ~2s there. A laptop profile understates the real number by
an order of magnitude, which is why the script prints both and why the fixes below are counted in
requests and COPYs rather than in a local stopwatch.

Cold, on `BUS_Bep_offensief_FX` (61 holdings, wrapped certificates), of ~2.9s local:

| loader | local | round trips | what it is |
|---|---|---|---|
| `_holding_risk` | 950 ms | 0 (3 COPY) | per-ISIN 5y vol / beta / 12-1 momentum |
| `_book_port_items` | 460 ms | **21** | the AIRS book: holdings, ledger, wrapped books |
| `index_returns` (×2) | 490 ms | 0 (3 COPY) | the benchmark over the two windows |
| `compute_portfolio_performance` | 400 ms | 2 | this model's own yfinance return |
| `_bench_start_caps` · `_grid` · `members` | 390 ms | 8 | the benchmark's constituents and caps |

### ⚠⚠ MOST OF THAT IS NOT ABOUT THE PORTFOLIO ON SCREEN — the cross-portfolio leg cache

`members`, `index_returns` and `_bench_start_caps` take a **label and a window**; they do not take a
portfolio, and every book on this page defaults to the same benchmark. A holding's vol/beta/momentum
is a property of the **instrument** — and the risk variants of a strategy (Defensief / Neutraal /
Beperkt Offensief / Offensief) hold nearly the same names, sitting next to each other in the table.
So opening books in turn recomputed answers already in memory.

`routers/_analysis_cache.py` grew a **second store** for these sub-results (`leg` / `leg_get_many` /
`leg_put_many`), on the **same data fingerprint** as the payload store — so a leg can never outlive
the payload cache's notion of "current", and the staleness argument in that module applies to both
unchanged. It is a separate store because the entry budgets differ by three orders of magnitude
(4,096 legs is a few MB; 4,096 payloads would be ~560MB).

Measured, same sequence, `--cold` vs warm:

| | round trips | COPY |
|---|---|---|
| first book opened | 42 | 28 |
| a **dissimilar** book after it | 34 → 34 | 24 → 20 |
| a **sibling variant** after it | 35 → **27** | 24 → **12** |

Locally that sibling case is ~2.0s → ~0.9s. The basket path (an unpaired book) has **no** payload
cache at all, so it gains on every single open: 396ms → 113ms.

⚠ **The win is in the SECOND book, not the first.** A cold server still pays the full load once.
See [`TODO.md`](../TODO.md) for the two ways to remove that too, with the numbers each would need.

⚠ **`date.today()` is in the key explicitly.** `index_returns` prices to "today", an input it takes
from the clock rather than the database — the one input the fingerprint cannot see.

⚠ **A hit is an answer, including an empty one.** A holding with too little history yields `{}`, and
that is a result: treating "no row" as "not computed" would make a book of young listings re-run the
whole five-year load on every open. `_holding_risk` stores the empty rows and drops them on the way
out, where callers have always expected "present ⇒ has at least one figure".

⚠⚠ **The miss path stays batched.** `_daily_eur` loads five years of closes for every holding in ONE
`COPY`; memoizing per ISIN with a plain `leg()` loop would serve the hits and then run that COPY
once per miss — ~60 round trips, making a "faster" modal slower on exactly the cold path. Hence
`leg_get_many` → compute the misses together → `leg_put_many`.

**Proof, not assertion**: `backend/scripts/verify_analysis_cache.py` recomputes every paired book
with the memo forced to always-miss and with it warm, and compares the payloads field for field
(`timings_ms` excluded — it is a stopwatch). 17 of 17 identical, plus the basket path.

### ⚠⚠ And one of those reads was silently truncating in production

`_airs_portfolio_perf.compute_portfolio_performance` read the WHOLE
`airs_model_portfolio_position` table with no `.range()` — **1,001 rows against PostgREST's
1,000-row cap on Supabase cloud** (10,000 locally). Complete on a laptop, one row short in
production, with no `ORDER BY` to make *which* row predictable: a model would quietly lose a
position from its priced basket and renormalise the return over the rest. Both its reads now go
through `routers/_airs_ref.py`'s canonical loaders, which page properly — which also makes them the
byte-identical request the modal already issues, so the per-request memo collapses them (−2 round
trips on every open). Output verified equal across all 61 portfolios to 13 significant figures; the
residual is float accumulation order, because the canonical read is `ORDER BY id` and the old one
was ordered by nothing.

---

## The benchmark tile: the index ETF, not our rebuild (2026-08-19)

`Return (YTD) € − vs ACWI return € = Excess` in the Analyse modal. The middle figure used to be
`_asset_benchmark.compute_index` — a cap-weighted index rebuilt from 1,514 constituents. It is now
the iShares MSCI ACWI ETF's own closing price series, from GuruFocus, converted to EUR.

### Why the rebuild was wrong, measured

| | |
|---|---|
| our reconstruction | **+11.83%** EUR |
| ACWI ETF, price return | +13.14% USD → **+14.67%** EUR |
| ACWI ETF, total return | +13.85% USD → +15.39% EUR |

The 2.8pp is structural, not noise, and none of it is fixable by arithmetic:

* **Full market cap, not free float.** MSCI float-adjusts; we weighted on `market_cap_eur`, the
  whole company. Saudi Aramco was **1.50%** of our index against a published **0.044%** — 34x,
  because only a sliver of Aramco floats. Saudi Arabia read 2.07% against a published 0.333%.
  NVIDIA was 3.76% against 4.95%, Tesla 2.00% against 1.14%. Re-weighting our own priced rows with
  the published iShares weights, on identical prices and window, moved the total from 13.13% to
  20.24% over the 84% that matched by name — indicative rather than exact (the bundled file is
  dated 15-Apr, so it embeds a quarter of drift), but it says the weighting dominates.
* **Coverage.** 1,678 priced of a 1,998-member universe, against ~2,270 lines in the real index. A
  cap-weighted rebuild does not lose the missing weight, it redistributes it.
* **No common as-of date and no staleness guard.** Every constituent ran to its OWN last close and
  `as_of` reported the maximum — on the local dataset, 58.5% of the weight was priced more than 30
  days before the date on the header.
* **10% of weight priced on the wrong venue's currency** (Alphabet on a EUR line at 3.82%, Eli
  Lilly on `LLY.SG`, IBM on `IBM.HM`), where `return_local == return_eur` and no FX leg applies.
* Membership is a static snapshot; mid-year index reviews are never replayed.

### ⚠ The canary, and why it was not optional

This repo's standing rule is that the GuruFocus legacy API **never 404s** — `stock/{sym}/<anything>`
returns 200 and a 46-point all-zero series — so "it returned data" proves nothing. It was probed
before anything was built: `stock/ACWI/__canary__` genuinely 404s (`Stock not found, exchange
[NAS], symbol [ACWI]`), and the inception dates confirm it independently, which a placebo cannot
fake — ACWI's first bar is 2008-03-28 (fund launched 26 Mar 2008), VT 2008-06-26, URTH 2012-01-12,
SPY 1993-01-29.

### ⚠ The two-vendor objection does not carry over

The panel was moved off GuruFocus in 2026-07 because RECONSTRUCTING an index from a vendor with
coverage holes silently redistributes the weight it cannot price (~7.8% of ACWI, 31.96% of the
AEX). That is an argument about summing 1,514 names, not about one US-listed line, which GuruFocus
serves in full. On a single ETF close the vendors agree to the cent, and the benchmark is still the
same KIND of number as the portfolio: an EUR price return over the same window. The comment in
`_returns` that stated the old reasoning has been rewritten rather than deleted, so the next reader
finds the argument and its refutation together.

### What is where

* `routers/_benchmark_etf.py` — `PROXY = {ACWI: ACWI, SP500: SPY}`. ⚠ **AEX has no entry**: every
  European UCITS line 404s (`IAEX`, `IWDA`, `IUSA` on AMS/XAMS), so it keeps the rebuild — the
  cheap one anyway (22 names, 2.3s) and the one whose 15% cap the rebuild already models.
* `_index_returns` falls back **per window, not per label** — a since-inception anchor can predate
  the fund — and runs the rebuild only for the windows still missing, so the normal ACWI case never
  touches the 1,678-constituent COPY at all.
* ⚠ **Two prices, not a series.** `_latest` / `_at_or_before` use `.limit(1)`; the first cut paged
  4,627 rows (258 ms, five round trips) to use the first and last of them, on the critical path of
  a modal that paints nothing until it is done. Three windows now cost 61 ms.
* Freshness: the `price_update` benchmarks phase calls `refresh_index_proxies()` daily;
  `ensure_fresh` repairs lazily, ⚠ at most one vendor call per ticker per process per day — a 1.35s
  GuruFocus round trip inside this modal is the whole of somebody's wait.
* It is a **price return**. Distributions are excluded, exactly as the rebuild's were, so the switch
  changed where the number comes from and not what kind of number it is. GF serves them
  (`stock/ACWI/dividend`, $1.0097 ex-15 Jun 2026, +0.71pp YTD) if a TR line is ever wanted.

### ⚠⚠ Two benchmark numbers now sit one click apart

The **Attribution panel still reconciles to the rebuild**, because it decomposes the index name by
name and an ETF price has no names in it. That is survivable only because both surfaces SAY which
one they are showing: the tile through `benchmarkSourceNote.ts` (fed by `benchmark_source` /
`benchmark_ticker` / `benchmark_ytd_as_of`), the panel through its existing `account_excess_pct` vs
sleeve `excess_pct` gap. The Scorecard's code used to assert that the Excess **equals** the
attribution Total; that claim is now false and is marked as such. Nothing may quietly restore it.

⚠ `benchmark_as_of` still describes the ATTRIBUTION's legs (yfinance rebuild) and was deliberately
not repointed — the tile's own as-of is `benchmark_ytd_as_of`.

### The two ⓘ badges on the Scorecard

Both return chips carry the standard `<Provenance>` badge (`benchmarkSourceNote.ts` builds the
benchmark card's four fields; pinned by its test). Three things there are load-bearing:

* ⚠⚠ **`benchmark_etf` is a source key of its own**, rendering "GuruFocus daily close (index ETF)"
  where `benchmark` renders "yfinance close (benchmark constituents)". Reusing `benchmark` would
  print one vendor's name over the other's number — the single mislabel the provenance badge exists
  to make impossible, on the one figure that can now come from either place.
* ⚠ **`how` is the rule, then the rule with this window's own numbers under it**, separated by a
  blank line — the shape `ProvenanceCard` already renders `whitespace-pre-wrap` for, and whose own
  comment asks for it:

  ```
  A formula on the data: (close ÷ FX_close) ÷ (open ÷ FX_open) − 1  —  close/open are
  ACWI's USD prices, FX is USD per EUR on that same day

  2025-12-31 → 2026-08-18
  (160.08 ÷ 1.1593) ÷ (141.49 ÷ 1.1750) − 1 = +14.67%
  ```

  The four marks ride on the payload (`benchmark_ytd_open_price` / `_close_price` / `_open_fx` /
  `_close_fx`). ⚠ **`*_fx` is the ETF currency PER EUR** (1.1750 USD/EUR) — the direction the
  formula divides by; quoting it the other way up would make the worked line wrong while the result
  stayed right, the hardest kind of error to notice. ⚠ It states the mark it OPENED on
  (2025-12-31), not the 1 January anchor: they differ by a trading day and a reader checking by
  hand against 1 January would pull the wrong bar. ⚠ It degrades to the rule ALONE when any mark is
  missing (an older payload, or the rebuild path) rather than printing `NaN ÷ undefined` — a worked
  example the reader cannot reproduce is worse than none, because it looks like proof. That is what
  the test asserts: it re-evaluates the printed line and checks it lands on the printed answer.
* ⚠ **The benchmark chip is wrapped in `ProvenanceFetchedAt at={undefined}`.** The subtree sits
  inside `at={holdings_fetched_at}` — when we last scanned THIS PORTFOLIO from AIRS, which says
  nothing about when we last read an index ETF price. `fetchedAt={null}` cannot express "do not
  inherit", because `??` treats null as absent by design.

⚠ `Chip` takes `prov` OR `hint`, never both: a native `title` waits ~1–2s and then paints its own
box over the popover the ⓘ just opened.

## Asset-class buckets (2026-08-18: `Equity ETF` retired)

Five buckets, each naming what a holding **invests in**: `Equity` (shown as **Stocks**) · `Bonds` · `Alternatives` · `Cash` · `Unclassified`. Declared once in `routers/_airs_holding_isin.py::BUCKET_ORDER`; the display label lives only in `frontend/.../allocationColors.ts::bucketLabel`.

- ⚠⚠ **`Equity ETF` was a sixth bucket and is gone.** It split the equity sleeve on the **wrapper** while every other bucket split on the underlying — a bond ETF has always been `Bonds`, never "ETF Bonds" — so the bar answered two questions at once and a book's equity exposure needed two slices added together. An equity ETF is now Stocks; a bond ETF is still Bonds.
- ⚠⚠ **Retiring it removed a guarantee that had to be replaced, not just deleted.** `bucket === 'Equity'` used to imply "an operating company", and every owner-earnings gate was written against it alone (a fund has no earnings, and this app deliberately does not look through funds). The fund-ness now travels on its own field — `BookHoldingDetail.is_fund`, set in `_airs_portfolio_analysis._reclassify_book_rows` — and **every such gate must test `!is_fund` as well**. ⚠ `holdings_detail` builds fresh dicts, so the flag has to be listed there explicitly or it is silently dropped and every `!h.is_fund` guard passes on `undefined`.
- ⚠ **The Analyse modal always shows Stocks · Bonds · Alternatives · Cash, in that fixed order, in BOTH the bar and the holdings table.** `_weigh_alloc` emits the four whatever the book holds (`_ALWAYS_SHOWN`); the table keeps their sections via `allocationColors.ALWAYS_SHOWN_BUCKETS`, which must mirror it. Two reasons an omitted class was wrong: it cannot state a zero (three rows where there were four reads as "bonds not computed" exactly as much as "no bonds held"), and the policy bands are drawn per bar — so a Defensief book holding **no** bonds against a 55% minimum had no bar to draw the breach on, making the largest possible violation the one the overlay could not show.
  - ⚠ The bar **no longer sorts largest-first**. Size-sorting reads well on one portfolio and badly on the job this modal is for: the classes sit at a different height per book, and move for the *same* book once a rebalance changes which is biggest. A fixed order makes vertical position carry the class. It is also the only way the 0.00% classes land sensibly — sorted by size they sink below Cash.
  - ⚠ `Unclassified` is **not** forced: it is our own failure to classify, so an empty one is good news, and printing it on every healthy book advertises a problem that does not exist. It appears the moment it has rows. A genuinely empty book still returns no slices at all.
  - ⚠ The forced zeros never enter the denominator — otherwise every real class's percentage would fall for being beside an empty one. Pinned in `test_airs_portfolio_analysis.py`.
- The policy grid (`_airs_allocation_bands.POLICY_BUCKETS`) is **4x4** — four risk profiles x Stocks/Bonds/Alternatives/**Cash**, in the same order the allocation bar uses, so a band and the bar it is drawn over cannot be read in two sequences. ⚠ **Cash was added 2026-08-18**, having been excluded on the grounds that it is the REMAINDER of the invested classes. True of the *default* and false of the *bounds*: "hold at most 10% cash" is a real mandate, it is the one most likely to be breached by drift rather than by decision, and with no band there was no line for the bar to breach — a fully-liquidated book showed 100% cash against no policy at all. Consequence: the defaults may now legitimately reach 100 where a total of 95 used to be ordinary, so the editor's sum note changed. Still shown, never enforced — an unset cell is null, and enforcing the total would make filling in the first cell an error until the last was typed. `Unclassified` stays out for a different reason than cash ever was: it is not a holding decision, it is our inability to see inside an instrument.
- Retiring `Equity ETF` also shrank the grid (it was 4x4 with an ETF column). Stored `Equity ETF` bands were folded by migration `20260818120000`: minima and defaults **summed**, maxima summed then **capped at 100** — a real edit, not arithmetic (Offensief was 80 + 25 = 105), raised as a `NOTICE` naming the profiles to review.
- Both `asset_bucket_override` and `airs_allocation_band` carry a CHECK constraint rejecting the retired key, so a stale deploy fails loudly instead of reintroducing a bucket nothing renders.


## Calendar-year history and finalisation

Performance has a reporting-year selector. The current year opens by default;
previous years retain all twelve month tiles. Annual headlines read AIRS's exact
`cumulatief_rendement` and a policy benchmark over the same funded-book window,
not a compound of rounded monthly observations. January does not reuse December's
return as the new YTD figure. Incomplete historical years show their actual end date
and an explicit year-end warning.

Apply `supabase/migrations/20261030040000_airs_year_archive.sql` **before deploying
this code**. It adds service-role-only annual transaction/journal archives and
backfills the existing caches. Old journals lack requested-window metadata, so
backfilled journal coverage remains explicitly unknown. New exports are archived
before the live cache is replaced; an archive failure leaves the old live cache
intact and fails the scheduled source step. Narrower exports cannot regress a
more complete archived window.

The existing post-AIRS job finalises the previous year: it saves a complete December
bundle and full January–December transaction and dividend exports after year end.
Historical exports do not write to the live YTD caches. It requires the final
weekday valuation, an AIRS cumulative return and successful report sections before
marking the year complete. Missing/late exports remain incomplete in the durable
receipt and freshness audit, and retry on the next daily run, including after a
subsequent year rollover. Complete receipts are skipped, and finalised December
bundles are protected against overwrite by daily preparation. This uses the existing
AIRS session lock within the daily workflow; it adds no second account/model scan
or independent cron job. The weekday cutoff does not infer a market holiday calendar;
missing final valuations remain explicitly pending.

## Full weekly AIRS instrument history

If market-risk preparation is interrupted by source updates, the risk bundle returns
an HTTP 202 update state with saved AIRS weekly histories still available. The client
retries after ten seconds and never caches that partial response as a complete bundle.
Returns and Weights remain usable while the market-risk views are being prepared.

The **Weights** tab immediately after Returns lists the saved AIRS instrument catalogue
with a **Fetch** action per row. Its configurable earliest date defaults to **1 February
2025**. Fetches start today and use the same weekly history loader and stopping rules
as Returns, with the selected date as an additional strict lower bound (including report
fallbacks and separate YTD-anchor requests). Existing older history is retained. Row
jobs queue through the shared AIRS session and report progress through the standard
task notifications; other instruments can be queued while one is fetching.

**Fetch all** uses one AIRS session and one full portfolio report per weekly date,
extracting and saving all instrument quotes in batches. Completed observations are reused
on subsequent runs. Each instrument stops independently after two consecutive zero-return
weeks; the remaining instruments continue to the date limit or the report boundary. The
separate YTD opening is also shared. Bulk progress uses the standard task notification;
Open remains available during the fetch, and the saved results reload in one bundle read.

### Allocation search in Weights

The profile selector and four min–max labels use the same versioned defaults plus saved
administrator overrides as Allocation bands. The account name supplies the initial
profile where recognised; otherwise the user must choose one. The server reloads the
selected profile policy for every search and rejects any grid candidate outside any
class's bounds. Defaults are targets, not additional optimisation constraints. Missing,
invalid or infeasible bounds block the search; there is no unconstrained fallback.
The current-allocation comparator retains the actual holdings, even if they breach
the policy, while all searched candidates must comply. Limits that admit no 5% grid
allocation produce an explicit unavailable result. The response includes the applied
policy so the displayed bounds are refreshed to the policy actually used.

**Find optimal weights** searches saved observations only and never writes allocations
or fetches reports. Defaults: Sharpe ranking, 2% annual Sortino target, 0% assumed annual
cash return and available-history proxy mode. The 5% long-only grid sums to 100% and
enforces the selected profile bounds. Negative holdings and unknown classes block it.
Asset classes reuse the Holdings overview's shared classification: reviewed overrides,
then the exactly linked AIRS model's category by ISIN, then the resolved holding class
and generic classifier fallback. No fuzzy portfolio matching or separate optimisation
classification table is used. AQR's reviewed Alternatives rule accepts the source name's
trailing full stop. The optimiser keeps direct instrument returns rather than expanding
certificate holdings into additional, duplicated return series.

Proxy mode reweights available instruments within each class for each weekly timestep,
using current EUR values. Missing returns are excluded, genuine zero observations kept.
Both prices use historical EUR FX at most seven days old. Actual marks must fall within
their respective weekly bins, never after the requested target, and span 4-10 days.
Actual dates may differ within a weekly bin; no interpolation is performed.

Weeks with an entirely unavailable held non-cash class are skipped. At least 4 usable
weeks allow an exploratory calculation, including short samples such as nine weeks;
this minimum is not a statistical-reliability threshold. Details reports average/minimum class coverage, instruments used
in at least one week, instruments excluded throughout and skipped weeks. The current
comparison retains actual full-book class weights using those same proxy returns.
Strict mode instead requires 52 consecutive weeks for the complete current basket.

Older observations rank candidates by the chosen ratio; the newest observations test only the
training winner. With at least 52 usable weeks, the split reserves at least 26 weeks
for validation while retaining at least 26 for training (74 becomes 48/26). Longer
histories retain the 75/25 split when it already provides 26 validation weeks. Shorter
exploratory samples use 75/25. The current allocation and top three training candidates
are shown with weights and training/test scores regardless of significance. Sharpe is
the default, with Sortino selectable in Settings. Sharpe uses sample deviation and
the assumed cash return as the risk-free rate; Sortino uses its separate target.
Undefined ratios display as a dash. Test data never selects or ranks candidates.
Details displays only full-period tables by class: Instrument, Sharpe, Start and End.
Training/test observations are combined before recalculating each instrument's Sharpe;
the displayed dates are the actual first opening and last closing marks used. Each class
ends with an **Average instrument Sharpe** row: the arithmetic mean of defined instrument
Sharpes, with the group's period envelope. This is a descriptive average, not the Sharpe
of a weighted class return series. Missing returns and undefined ratios are excluded from
the mean; undefined values display a dash. No diagnostic prose is shown. The optimiser's
training/test search and weighted class calculations remain unchanged. **Download Excel**
exports the displayed full-period results as XLSX using the shared application exporter:
Category, Instrument, Sharpe, Start and End, including each category average row.
Scores remain numeric (rounded to two decimals), missing values are blank, and dates
match the displayed `1 nov 2026` format. Export uses the existing result without fetching
again and is hidden when inputs change or instrument scores are unavailable.
The paired moving-block bootstrap requires 26 validation weeks (and eight downside
observations per series for Sortino), plus a positive one-sided 95% lower bound on
improvement in the selected ratio (999 samples, 4-week blocks,
at least 950 defined samples). Gapped samples can be searched but cannot pass this
check; compounded return and drawdown metrics are withheld to avoid compressing
missing market periods into a continuous backtest. Settings and diagnostics collapse.

These are historical checks under sampling assumptions, not proof of a strategic
optimum. Prices exclude dividends, coupons, corporate-action adjustment and costs.
Current-holdings selection bias and changing proxy composition remain material limits.
Cash is an assumption, not a measured instrument return. No allocation is applied.
Short-sample calculations explicitly report their usable week count and unsupported
validation state as context without hiding candidate weights. If no
calculation is possible, the specific server reason is shown rather than a generic error.

The Returns tab's **Load full history** action starts at today's Amsterdam calendar
date and steps backwards by exactly seven days for the selected instrument. Existing
quotes are reused only for an exact quote date or a previously checked requested date;
an unpublished report falls back up to six calendar days, including across weekends. The search stops
after two consecutive 0% weekly quoted-price returns, or when no AIRS report is available
within the requested week. The zero-return rule is an explicit user-selected heuristic.
The same cutoff trims previously saved flat tails from the displayed full history without
deleting source observations. YTD remains independently calculated. A report where the selected
instrument is absent still counts as an available report, so sales do not prematurely end
discovery. Login,
transport and unparseable reports fail the job instead of being mistaken for history exhaustion.

Each observation is committed to the existing per-instrument snapshot table before progress
is published. Cancellation retains those observations; subsequent loads reuse saved quotes
and completed historical no-data checks. Historical quotes never replace live holdings or
publish incomplete portfolio snapshots. No migration is required. A 1900 safety boundary fails
explicitly if reached without a stopping boundary.

Charts display only valid non-zero weekly returns, with no placeholder bars for missing,
zero or unheld observations. Dates and cells remain aligned after filtering. **YTD** uses a separate
31 December opening observation for the current year, while **Full history** uses the first
and last actual instrument quotes. The YTD opening is checked after the backwards scan. YTD is unavailable
when its own opening or closing quote is missing, regardless of older available history.

## External-fund region allocation

The external-fund allocation modal has Sector and Region tabs using one shared allocation
table. Region loads on demand from `/api/airs/etf/{isin}/region-allocation`. Physical iShares
funds use the official embedded geography/countries breakdown, falling back to justETF's
expanded Countries table. Other registered ETFs use justETF. Synthetic funds retain the
index-facing justETF source. Yahoo-only mutual-fund sector coverage does not imply regional
coverage; unavailable country data is reported explicitly.

Country names map through the existing `asset_pipeline.geo.msci_region_of` definitions into
North America, Europe, Pacific and Emerging Markets. Provider Other, unknown countries and
non-country buckets remain visible as Unclassified with their original source labels and
weights. They are included in the allocation total, never redistributed over known regions.
The table retains original country weights, grouped regional weights, current fund weight and
portfolio contribution, with the same rounding normalization as Sector. Each tab displays its
own source and as-of date. Incomplete/invalid totals are rejected and successful regional
responses are cached separately for six hours.
The main Region comparison chart has an **Add funds** toggle. It loads the regional breakdowns
of supported funds in the selected asset class on demand, then adds their underlying regional
contributions to the existing whole-portfolio bars. It reuses the Sector weighting arithmetic,
keeps the benchmark unchanged, includes Unclassified, and restores the original rows when
disabled. Missing breakdowns are counted as unavailable; successfully loaded data is retained
when toggling off and on.

## Monthly benchmark performance

Insights → **Monthly performance** compares a selected linked Leonteq portfolio with ACWI
(default), S&P 500, global bonds or EUR high yield in grouped monthly bars. Portfolio,
benchmark and year selectors preserve the existing Insights navigation; an expandable values
table provides exact returns and Leonteq-minus-benchmark differences in percentage points.
Opening the view reads saved Leonteq performance and the existing EUR benchmark ETF price
series only. **Refresh returns** checks benchmark prices and reuses the shared monthly-return
background job, with progress notifications. An explicit benchmark refresh bypasses an earlier
same-day check so a newly published completed close can be picked up immediately.

The current month is included as MTD, using Leonteq's reported monthly return and the benchmark's
latest stored completed close through today. Future months stay empty. A cached MTD observation
remains unavailable after that month ends until Leonteq is fetched again. Benchmark returns unchain consecutive
month-end cumulative EUR marks, with each price converted at its historical FX rate. Missing
boundaries, FX or stale month-end prices remain unavailable; they never become zero returns.
The footer shows the benchmark and Leonteq fetch timestamps separately.

## Recent-transaction weight reviews

Insights → Rebalance has Full portfolio and Recent transactions views. Recent transactions
reads the authenticated LYNQS `index-transactions` endpoint with its observed offset/limit/sort
parameters. Every page is checked for a stable total and non-overlapping transaction IDs;
incomplete feeds fail closed. Only booked security Buy/Sell events enter the queue; fees,
currency movements and explicitly pending/cancelled records are excluded.

The user selects a Fixed connection and a 7/30/90-day window. Several trades in one ISIN
become one review row. Current target weights come from a fresh, complete Leonteq composition,
not from trade amounts. Matching reuses the Connections matcher, including manual rules,
company-exposure links and cash handling. Other unmatched instruments do not block a valid
individual row. A zero target requires an absent holding, latest Sell events, a complete source
snapshot dated at or after the trades, and one identifiable AIRS Fixed row.

Dummy mode is the default. A live per-row apply re-fetches the trades, connection rules and both
compositions; changed trade IDs, targets, previous weights, row identity or composition date
require another review. The existing AIRS percentage endpoint performs the one write and reads
back the saved export. Uncertain outcomes cannot be retried until the queue is fetched again.
The review displays the projected portfolio total; individual changes can temporarily move it
away from 100%. Fetching transactions and checking the feed never execute Leonteq trades or
write AIRS weights. No new schedule or database migration is required.
Percentage updates are idempotent when a fresh AIRS export already contains the requested
weight. Revert then reports “already restored” without submitting another save; any different
unexpected current weight still raises a conflict.

Below the transaction rows, **Balance cash to 100%** provides **Read latest total** and
**Execute**. The read fetches the latest AIRS Fixed snapshot and its native `.tableTotal`
footer, validates the footer against the complete exported weights, and proposes
`cash + 100 − total`. Export precision is retained even though the footer displays two decimals.
Exactly one editable Liquiditeiten / cash / Effectenrekening row is required; missing weights,
conflicting totals, ambiguous cash or a target outside 0–100% stop the action. Live execution
rechecks the date, total, cash row and weights under the AIRS session lock, submits once through
the shared native percentage editor, and verifies a 100% total with all other holdings unchanged.
Dummy mode simulates without saving. Individual Execute buttons own their progress state;
additional row actions queue sequentially so AIRS writes do not overlap.
The cash section also compares combined AIRS and Leonteq cash weights side by side, showing
the signed AIRS-minus-Leonteq difference in percentage points. Leonteq comes from the fetched
composition; reading the latest total or saving cash updates the AIRS side. Missing weights
remain unavailable rather than being treated as zero, and the comparison stays visible after saving.

A current Leonteq holding absent from AIRS Fixed is offered as a new position at its current
weight. Existing manual connection/disconnection rules still take precedence. A live Add uses
AIRS's native `toevoegen` form and its ISIN autocomplete to select an already registered Fonds,
then reads the export back to verify one new row with the same ISIN, registered name and weight.
The server rechecks the Fixed connection and current Leonteq target, checks for duplicates, and
disables automatic retries around the submission. Missing or ambiguous instrument registration
stops before saving and tells the user to register/review the instrument manually. No instrument
master records are created. Returning to Connections reloads its saved-data comparison so the
new position appears as connected.

## Price checks

Insights → Prices resolves missing or currency-mismatched Yahoo listings using ISIN identity
from OpenFIGI, Yahoo search and chart metadata. Candidates must belong to the FIGI listing
set, match the issuer and source currency, and have valid prices. Confirmed mappings persist
in `leonteq_price_symbol`, keyed by ISIN and normalized currency; this does not change the
general analysis/execution listing. Apply `20261109160000_leonteq_price_symbols.sql` before
deploying. RLS restricts access to the backend service role.

Each row has Recheck: it reads that source's fresh holdings for the saved Dynamic connection,
matches the selected instrument, retries symbol resolution and updates that row. Client prices
are never accepted. Effectenrekening is always cash, including when it carries an identifier.
Ambiguous identities, missing prices, different currencies and different valuation dates remain
uncheckable. A matching listing on another exchange can still have a different closing price.

## Diagnosing missing AIRS valuations

In `airs_vermogen._vermogen_most_recent`, a failed valuation lookup initially applies only to the account that made it. A shared miss can skip a date only after the quorum is met and the date is newer than the latest proven valuation. The memo expires on the read path so single-account refreshes can see valuations published after a fleet scan. The saved `reports_ok` badge clears when a refresh successfully retrieves the report. See [diagnose_missing_reports.py](../backend/scripts/diagnose_missing_reports.py) and the valuation-discovery tests in `test_airs_html_fragment.py`.

Analyse opens with the AIRS cumulative book-return series. Performance and Risk load their respective bundles when opened. The current book-return chart uses the dated AIRS observations. It does not derive returns from holdings values.
