# Benchmark Factsheet

The separate sidebar entry opens `/factsheet`. The table
shows fifteen-year EUR total return, CAGR and daily annualized volatility for ACWI,
SYBA.DE (State Street SPDR Bloomberg Euro Aggregate Bond UCITS ETF, ISIN
IE00B41RYL63), QAI (NYLIM Hedge Multi-Strategy Tracker ETF, a liquid-alternatives
proxy), and HDG (ProShares Hedge Replication ETF, an HFRI-oriented replication proxy). These are ETF returns after fund expenses.

The bond reference is the user's justETF profile:
https://www.justetf.com/en/etf-profile.html?isin=IE00B41RYL63#overview
The verified Yahoo listing is SYBA.DE, quoted in EUR. It replaces AGG on the
Factsheet only. The snapshot key includes this asset selection, so old AGG
snapshots are never reused. The underlying fund launched in May 2011; shorter
listing histories are labelled separately rather than filled from another fund.

Sources for calculations remain Yahoo completed **adjusted** daily closes,
including reinvested distributions. ACWI, QAI and HDG use historical USD/EUR from
the existing FX store; SYBA's EUR prices require no FX conversion. The bond row
links to justETF for the fund reference and Yahoo for historical price data.
Missing adjusted closes, FX or daily coverage produces an explicit error.
Raw source observations remain in the saved snapshot for audit but are excluded
from the HTTP response.

All four funds use the latest shared completed closing date. A full fifteen-year opening
uses the latest close on/before its calendar anniversary, no more than seven
days earlier. Where all have full history, they share that opening date too.
CAGR uses actual elapsed years (`days / 365.2425`). Volatility is sample standard
deviation of simple daily EUR returns multiplied by `sqrt(252)`. Each instrument
uses its own trading dates without filling market holidays. More than ten days
between observations, or fewer than 200 returns per elapsed year, is insufficient.

Apply `supabase/migrations/20261113200000_benchmark_factsheet.sql` before rollout.
GET reads the saved snapshot. Opening missing/stale data requests preparation
through POST; Refresh forces a new calculation. A daily scheduler job,
`benchmark_factsheet_daily`, runs at 23:45 UTC. Preparation downloads only the
four ETF series (fifteen years plus an opening-date buffer). Provider/calculation
failure leaves the prior complete snapshot unchanged. FX synchronization must
already cover the window. Direct table access is service-role only; authenticated
dashboard users can read and refresh through the existing authorization tier.

HDG appears immediately after QAI and uses the same shared EUR calculation
window, adjusted-close methodology and daily volatility convention. Both are
replication ETF proxies, not the HFRI index itself. The cache key includes HDG,
so previously saved three-fund snapshots are not reused.
