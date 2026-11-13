# Portfolio valuation

The standalone `/valuation` page reads saved daily tables. Select a portfolio
from the same named account roster as the dashboard. Its linked Fixed
model supplies the valuation holdings. Unlinked accounts stay visible but cannot
be selected until connected on the dashboard. Linked certificates use the existing model look-through; funds,
cash and bonds are excluded. Repeated holdings with the same ISIN are combined.
Prices and fundamentals require an exact company ISIN; issuer and annotation
aliases never substitute an ordinary share's EPS for an ADR or another class.

The estimate is the five-year median positive daily P/E multiplied by the latest
reported trailing-year EPS **without non-recurring items**, not analyst forecasts.
Discount = `100 × (1 − current price / estimate)`; this differs from upside.
Quote units such as GBp are normalized. Different or unknown quote/earnings
currencies are unavailable; this version does not guess FX or ADR ratios.

Four consecutive quarterly observations form each trailing year. Missing quarters
are not filled. Annual/semiannual-only reporters are unavailable in this first
version. Historical earnings take effect 120 days after fiscal period end because
the stored fundamentals lack trustworthy original publication dates. This lag is
an approximation, not a guarantee against look-ahead. Vendor history may be
restated; the screen is a historical valuation reference, not a backtest or an
intrinsic fair value calculation. Current reported earnings are used once stored.

EPS is taken from one complete metric-code series per company. The newest actual
fiscal period determines the series; ties prefer the current `per_share_data_array`
schema over legacy spellings. Historical rows from other spellings are never
spliced in, since vendor restatements/split bases can differ. Genuine conflicting
observations within that series make only that company's valuation unavailable;
its price and other companies' valuations remain visible. The saved row records
the selected EPS metric code for diagnosis.

The five-year window requires at least 1,000 positive-earnings daily observations
spanning 1,460 days. Latest closes older than seven calendar days, EPS periods
older than 220 days, conflicting observations, nonpositive current EPS and missing
exact company mappings all produce explicit reasons. This model is unsuitable for
some industries even with full data; it does not establish that a company is a buy.

## Rollout and operations

1. Apply `supabase/migrations/20261113190000_portfolio_valuation.sql`.
2. Deploy backend and frontend together, including generated API contracts.
3. Opening an unprepared or stale portfolio prepares it through the selected-
   portfolio POST endpoint. Refresh forces a recalculation. The fleet also runs
   daily at 23:15 UTC through `portfolio_valuation_daily`; it can be run from Schedule.

The job reads the existing saved Yahoo prices and GuruFocus quarterly fundamentals.
Their normal market/fundamentals refresh jobs must be enabled; valuation does not
spend provider quota or scrape AIRS. A daily calculation does not mean its inputs
are dated today: price date, EPS period, composition date and calculation time are
available separately on hover. Refresh recalculates from the saved source inputs;
it does not fetch Yahoo or GuruFocus data itself.

The compact table keeps dates, method details and missing-input reasons on hover.
Before the first valuation publication, the read endpoint still returns the saved
composition's companies, with pending values. It does not calculate or fetch prices
to produce this initial stock list. The client then requests preparation when the
bundle is missing or stale, keeping the stock list visible during calculation.
Concurrent selected-portfolio preparations share a per-model publication lock;
a second initial preparation reuses the completed fresh result. Scheduled
publications use the same lock. GET requests never initiate preparation.

Missing financial data is a saved unavailable result, not a failed job. A database
or computation error prevents replacement of that portfolio's previous bundle;
the job receipt identifies the portfolio and ISIN. Completed portfolio bundles
remain saved if a later portfolio fails or the job is cancelled. Cancellation is
checked between companies and immediately before publication. Retry after fixing
the underlying source/mapping error. Reads never initiate preparation.

The migration enables RLS and reserves direct table access for service_role.
Authenticated dashboard readers use the existing AIRS read authorization.
