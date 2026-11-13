# Benchmark Factsheet

The separate sidebar entry opens `/factsheet`. The table
shows fifteen-year EUR total return, CAGR and daily annualized volatility for ACWI
and AGG (iShares Core U.S. Aggregate Bond ETF). These are ETF returns, after fund expenses,
not a reconstruction of the underlying index.

AGG is a U.S. investment-grade aggregate bond proxy with fund history since 2003.
It replaces AGGH.AS on this Factsheet only; the management dashboard's global
EUR-hedged bond benchmark is unchanged. The snapshot key includes the asset
selection and window length, so earlier AGGH or ten-year snapshots are not reused. If source data
is too short, fifteen-year cells stay blank and available history is labelled separately.

Sources: Yahoo completed **adjusted** daily closes (distributions reinvested),
with historical USD/EUR from the existing FX store. Both ETFs are converted at each
date's own FX rate. This includes USD/EUR exposure and does not currency-hedge AGG. Missing FX,
adjusted closes or daily coverage produces an error, never price-only or USD
figures labelled as EUR. Raw source observations remain in the saved snapshot
for audit but are excluded from the HTTP response.

Both funds use the latest shared completed closing date. A full fifteen-year opening
uses the latest close on/before its calendar anniversary, no more than seven
days earlier. Where both have full history, they share that opening date too.
CAGR uses actual elapsed years (`days / 365.2425`). Volatility is sample standard
deviation of simple daily EUR returns multiplied by `sqrt(252)`. Each instrument
uses its own trading dates without filling market holidays. More than ten days
between observations, or fewer than 200 returns per elapsed year, is insufficient.

Apply `supabase/migrations/20261113200000_benchmark_factsheet.sql` before rollout.
GET reads the saved snapshot. Opening missing/stale data requests preparation
through POST; Refresh forces a new calculation. A daily scheduler job,
`benchmark_factsheet_daily`, runs at 23:45 UTC. Preparation downloads only the
two ETF series (fifteen years plus an opening-date buffer). Provider/calculation
failure leaves the prior complete snapshot unchanged. FX synchronization must
already cover the window. Direct table access is service-role only; authenticated
dashboard users can read and refresh through the existing authorization tier.
