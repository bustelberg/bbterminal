# Saved benchmark charts

The Graphs tab reads one `/api/benchmarks/index/{label}/chart-snapshot?cadence=annual|quarterly`
bundle for all benchmark charts. The response contains completed growth series, ratio series for
every toggle, reporting counts and the build timestamp. It contains no company input rows.
Company comparisons and the calculation drill-downs still use their detailed inputs.

`benchmark_chart_snapshot` stores complete bundles in PostgreSQL, so server restarts and cache
expiration do not put expensive company reads back on the chart request. A missing snapshot
returns a preparation message; it never calculates thousands of company rows in the page request.

The existing background prewarmer publishes a replacement after startup and after fundamental
data invalidation. The previous result remains readable throughout the rebuild. Publication is
atomic, refuses incomplete endpoint sets, and does not allow a rebuild that started earlier to
overwrite a newer one. `BLEND_PREWARM` controls the index/cadence targets; both cadences for ACWI,
SP500 and AEX are included by default. A disabled prewarmer continues serving the last saved result.

Apply `20261103123000_benchmark_chart_snapshots.sql` and
`20261103124500_benchmark_chart_statistics_reads.sql` before deploying these readers. To seed or
rebuild explicitly, run from `backend`:

```powershell
.venv/Scripts/python.exe scripts/rebuild_benchmark_snapshots.py ACWI
```

This reads existing database figures only and makes no vendor API requests. The command prints
build time, snapshot read time and response size. The normal background worker uses the same build
and publication code. Apply future calculation changes with a new snapshot `VERSION` and rebuild
before switching readers to that version.

The Python ratio aggregation is checked against the existing TypeScript chart calculations using
shared annual and quarterly fixtures, including missing data, zero/negative figures, historical
market-cap weights, carry-forward and both SBC settings. ACWI still needs 500 usable reporters per
metric and period; saving a series does not relax its coverage rules.

## Weekly coverage and atomic statistics

`acwi_weekly_coverage` runs every Saturday at 03:00 Europe/Amsterdam. It checks the full current
membership, skips unsubscribed or recently checked feeds, retries eligible statements and analyst
estimates, and includes key ratios. Admission reserves 2,000 vendor calls per region, matching the
daily fundamentals job. A separate PostgreSQL advisory lock prevents replicas from spending quota
on the same weekly run concurrently. The scheduled-job history records acquisition outcomes,
quota deferrals, before/after coverage and both published snapshot revisions. The six-day recent
check cutoff ensures a job running a few minutes later next week does not miss that week's work.

Empty vendor responses remain coverage gaps, not invented zero values or a reason to abandon
later companies. Latest-year reporting and usable chart membership are reported separately:
an unreported fiscal year or a non-positive-history exclusion cannot be repaired by pretending
that the company reported usable data.

Snapshot version 2 stores R², endpoint CAGR and regression coefficients for every reported
comparison window, excluding LTM and forecasts. `read_benchmark_chart_snapshot` chooses the matching
saved window inside PostgreSQL, so large regression lookup tables do not travel to the browser.

The growth charts use one POST `/api/earnings/chart-bundle`. It returns the company/portfolio line,
the saved comparison line, their statistics on exactly the same date window, and the benchmark
snapshot revision. Nothing is returned until both sides are complete. The frontend commits that
bundle once and rejects results belonging to an older company, benchmark or cadence selection.
Derived cards also wait for both lines before displaying their values. Opening a chart no longer
starts a vendor-acquisition job; acquisition is scheduled server work.
