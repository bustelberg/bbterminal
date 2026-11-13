# Refresh troubleshooting

Use this guide for failed jobs, stale figures, lock contention, and saved partial
results. Configuration: [Environment setup](environment.md). Scheduling details:
[Scheduler](scheduler.md). Job declarations: `backend/scheduled_jobs.py`.
For a developer walkthrough of worker dispatch and lock boundaries, see
[Execution paths](execution-paths.md).

## Start with the receipt

1. In `/schedule`, open the failed job and copy its report. Record job ID,
   start/finish time, status, causes, affected portfolios/ISINs, and the full
   receipt when the compact report says more causes were omitted.
2. Check the deployed revision. New diagnostic messages only appear after
   deployment and another run; historical receipts are not rewritten.
3. Follow the nested failing stage. `airs_vermogen_refresh` can finish its AIRS
   scan and still fail because `dashboard_market` or `dashboard_post_airs`
   failed. Those child jobs retain their own receipts. Avoid repeating the
   entire AIRS scan for a Yahoo-price or Analyse-only problem.
4. Compare source dates, not just “job finished”. Keep ISO timestamp offsets;
   the AIRS daily scan is scheduled at 11:00 Europe/Amsterdam, while other jobs
   may use UTC. See the live next-run time for the actual schedule.

Scheduled receipts live in `scheduled_job_run`; ingestion/pipeline runs use
`ingest_run` and phase steps. User-started job progress lives in process memory
and does not survive a server restart, although completed database writes do.
Copy useful diagnostics before restarting. Do not include credentials or raw
environment files in a report.

## Identify the symptom

| Message or symptom | Meaning | Next step |
| --- | --- | --- |
| `price dated …; expected …` / `still dated …` | Stored close is behind the comparison date. A successful HTTP call alone does not make it current. | Inspect symbol, analysis ID, stored/expected dates, `vendor_latest_close`, and provider response evidence. Follow the price cases below. |
| Yahoo has no newer completed close | Provider check found no later completed close for that listing; this is not proof the mapping is correct. | Retry after publication. If persistent, check listing identity, trading calendar, and provider history. Months-old closes need investigation, not repeated blind retries. |
| Yahoo offers a newer close that is not stored | Provider and storage disagree. The recovery attempt may also have failed. | Inspect the instrument's write error in `asset_price_refresh`; resolve persistence/mapping problems and rerun that job. |
| Empty Yahoo response, HTTP error, throttling | Provider availability is unknown or temporarily blocked. | Keep saved data; wait for recovery, then retry the failed price job. Review the symbol and prices-phase steps if repeated. Do not treat this as “no newer close”. |
| `Saved a partial report` / position return unavailable | Usable sources were saved, but some returns cannot be established safely. | Open the month and inspect the position evidence/reason below. Missing is not zero. |
| `Analyse: older bundle still serving` | Saved overview does not match current source revision, code, or calculation day. | Check stored/expected metadata and cache key. Finish source jobs, then run `analyse_prepare`; use `dashboard_post_airs` for a full dashboard preparation pass. |
| Analyse metadata unavailable / missing snapshot | Storage access failed or no complete overview is published. | Fix the recorded database/configuration error, then prepare again. A small `analyse_prepare` batch may not cover the whole roster. |
| `all … workers are busy` | Scheduler admission rejected this tick; it did not start an extra queued worker. | Inspect running jobs and their last progress. Recurring jobs can try on a later tick; verify one-shot work actually gets rescheduled before assuming it will run. |
| Another daily refresh / preparation worker holds the lock | Another process owns the database advisory lock. | Let the active owner finish. Inspect its receipt before retrying; do not manually clear locks or launch parallel copies. |
| AIRS session busy | A scan owns the shared in-process browser session. | Wait for that scan to finish or cancel cooperatively, then retry. |
| `AdminShutdown`, SQLSTATE `57P01` during lock check | Database session was terminated; lock ownership could not be checked. | Check database availability/restart status. Background work deliberately defers until a later successful lock check; never bypass the check. |

## Partial monthly returns

The error includes position name/ISIN, opening/closing marks and dates,
quantities, trade activity, and source-section status. Saved source sections
and position diagnostics live in the month's `airs_monthly_report` bundle.

| Reason | Check / recovery |
| --- | --- |
| `reports_missing` | Missing sections, section error messages, or unreadable transactions. Repair access/parser issues, then reload the dated month. |
| `marks_missing` | Missing/invalid EUR values, quantities, or dated buy/sell execution marks. Verify the source export before retrying. |
| `quantity_not_reconciled` | Opening quantity plus buys minus sells differs from closing quantity, or a trade lacks quantity. Check transfers, splits, and transaction parsing. |
| `ambiguous_position` | Duplicate valuation lines cannot safely identify principal versus interest. Check component labels and the indicated valuation dates. |
| `identity_changed` | ISIN or currency differs between comparable marks. Verify the security identity. |
| `unclassified_activity` | Transaction type/date cannot be classified safely. Inspect the named or unassigned transactions. |
| `holding_period_discontinuous` | A sale and repurchase interrupt ownership. One continuous holding-period return is intentionally unavailable; another download may not change this. |

Zero failed AIRS sections does not mean all position returns are computable.
Never force reconciliation by substituting zeros, annual returns, or unrelated
price histories. Preserve the saved partial report while fixing its cause.

## Retry and lock behavior

- Yahoo's process-local circuit normally opens for 90 seconds after a confirmed
  ban, with recovery backoff up to 900 seconds. Deferred instruments remain
  incomplete. Repeated clicks do not bypass provider recovery.
- Held-strategy price retries (`price_update_retry`) default to a 3-hour delay
  and a budget of 3 per UTC day (`PRICE_RETRY_DELAY_HOURS`,
  `PRICE_RETRY_MAX_PER_DAY`). Counters are process-local; restarts reset them.
  These retries are not a guarantee that every dashboard instrument is repaired.
- Automatic AIRS daily attempts are limited to one per Amsterdam day by the
  daily lease logic, including failed attempts. A deliberate manual retry remains
  possible. One enabled scheduler replica is the supported operating setup.
- Daily dashboard work and Analyse preparation use separate Postgres advisory
  locks. The ingestion pipeline and AIRS browser also have process-local locks;
  they do not provide universal cross-replica serialization.
- Cancel is cooperative: it takes effect at safe boundaries. Closing a tab or
  disconnecting the event stream does not cancel the job. A hung worker retains
  its slot until it exits; capture evidence before considering a restart.

## Recover and verify

Repair the narrowest failed dependency first: provider/database or dated AIRS
source → affected source job (`asset_price_refresh`, `fx_sync`, or benchmark
job) → saved comparisons/reports (`airs_price_checks`, dated report reload, or
`dashboard_post_airs`) → Analyse preparation → `dashboard_freshness`.
Skip stages unrelated to the failure. These are application operations against
the selected environment, not unit tests.

The freshness audit reads actual source dates and completeness; it may repair
Analyse publication when that is the only remaining issue. “116 moved, 0 failed”
can still leave six stale instruments. Verify remaining-stale/deferred counts,
per-account issues and all issue groups, plus the affected UI's as-of dates.
Keep the old receipt and the new run ID when escalating an unresolved failure.

## Leonteq / AIRS price checks

A completed check with uncheckable comparisons reports **Completed with incomplete
coverage**. Differences over 5% are findings. A failed source refresh still fails
the job while retaining the partial result. Counts are source-price comparisons,
so one instrument can occur in multiple portfolios and sources.

The `ObligatieTopSelectie` → `OBTopSelectie_DYN` connection is an explicit
exception: no Yahoo lookup is performed. Match holdings by ISIN (or exact
normalized name when both lack an ISIN), normalize minor quote units, and compare
`(Leonteq / AIRS - 1) × 100`. A matched pair counts once and alerts only when the
absolute difference exceeds 5%. Missing matches, invalid prices, different
currencies or mismatched valuation dates remain uncheckable. Its row recheck
reads both sources, including when initiated for one source. Other portfolios
continue comparing with Yahoo; unique view keeps these two comparison bases separate.

In **Prices → View causes**, expand a category for compact instrument summaries;
hover a summary for ISIN, Yahoo symbol, portfolio, source, dated prices and the
full reason. Unique view combines repeated entries. The shared job details show
category counts; holding evidence stays on the admin-only result route.

| Cause | Next step |
| --- | --- |
| `valuation_date_mismatch` | Compare the source date with the prior Yahoo close. Check for a market closure or a carried-forward source valuation; never use a later close to fill the gap. |
| `listing_unresolved` | Read the resolver evidence: missing OpenFIGI identity, no candidates, rejected currency/issuer, or absent positive closes. Verify the exact security and currency before adding a mapping. A bond must not be replaced with its issuer's stock. |
| `missing_isin` / `missing_currency` | Repair source metadata or add an explicit verified listing for the named holding and currency. |
| `yahoo_response_unavailable` / `yahoo_request_failed` | Review HTTP status, chart error code or exception type. Retry transient failures; verify the listing for persistent missing charts. |
| `no_yahoo_closes` / `no_prior_close` | Check Yahoo history coverage and the source date. A usable chart with no completed close differs from a failed request. |
| `currency_mismatch` / `yahoo_symbol_mismatch` | Correct the listing or source currency; incompatible quotes remain uncheckable. |
| `invalid_price` / `missing_source_date` | Repair missing or invalid source data and rerun. |

Use the instrument's single **Recheck** button after repair. In the default unique
view it refreshes every represented portfolio and source. An old receipt containing
only totals cannot establish the individual causes; retain the new dated result.


## Returns tab: weekly instrument bundles

Apply `supabase/migrations/20261113170000_weekly_returns_bundles.sql` before deploying the bundle reader. Run `airs_weekly_returns` once from the admin Schedule page to seed existing dashboard accounts. Its normal timer is Sunday at 04:00 Europe/Amsterdam. It resumes shared AIRS weekly history from 1 February 2025, using the existing AIRS session and daily refresh locks. Collection commits observations between weeks; cancellation releases the session and keeps committed observations.

Daily `dashboard_post_airs` republishes each portfolio's complete saved-data bundle in `airs_weekly_returns_bundle` without scraping historical reports. The weekly collector populates missing history and then republishes the bundle. A single atomic upsert replaces each account's publication. A failed build retains the previous publication; inspect the scheduled job's per-account errors before retrying. Published series preserve unavailable prices as gaps.

`GET /api/airs/accounts/{portefeuille}/weekly-returns/all` reads one saved portfolio bundle. The Risk bundle can include the same saved payload, so the Returns tab reuses it without another request. Both instrument selectors use the bundled `instruments` dictionary. Opening the tab and switching instruments do not scrape AIRS or calculate series. If there is no prepared bundle, the response explicitly says preparation is needed. Missing bundle storage does not block other Risk views. A manual history job republishes saved observations and the browser reloads one complete bundle after completion.


## Daily valuation unavailable

For the Management Dashboard's daily valuation tables, missing earnings/history,
rollout migration and `portfolio_valuation_daily` receipts, see
[Portfolio valuation](portfolio-valuation.md). Annotation aliases described below
do not supply a valuation price or an ADR earnings conversion.

## Notes missing for an ADR or alternate ISIN

Notes resolve existing company records through exact ISINs, verified canonical alias chains, and the reviewed issuer links in `backend/services/company_notes_identity.py`. Exchange/ticker links also require the expected issuer name. These links apply to annotations and sector overrides only; they do not select another price series or change the holding's currency. Missing or ambiguous company records are never created or merged automatically.

The Notes editor uses `/api/companies/by-isin/{isin}/investment-note`. A stock without a vendor company record can still store private ISIN-based notes after applying `supabase/migrations/20261113180000_instrument_investment_notes.sql`. Existing company notes remain readable; verified ADR aliases share the company note. When a company record is linked later, the newest saved user note remains available and subsequent saves use the company record. Every read and write is scoped to the signed-in user.

For a read-only coverage audit across current account holdings and saved model compositions, run from `backend/`:

```powershell
uv run python scripts/audit_portfolio_notes.py --db-env-file ../scripts/.env.local --output ../notes-coverage-audit.json
```

The supplied file must contain `PROD_DB_URL`, `SUPABASE_DB_URL`, or `DATABASE_URL`. The audit enforces a read-only transaction and never reads private note contents. A failed connection is an incomplete audit, not evidence that there are no missing links. The 13 November 2026 audit found 215 distinct stocks: 193 existing company links, nine reviewed links, and 13 stocks requiring ISIN-based note storage; no remaining blocked identified stocks. A separate Brown & Brown model row has no ISIN and needs correction in AIRS. The report excludes fund/certificate wrappers and bonds.
