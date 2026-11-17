# Portfolio valuation

## Workbook model

`/valuation` now defaults to the model from `test.xlsx`, with five workbook tabs:
Onderneming (the default), Koers & winst, Waardering & koopzone, Scenario 2029 and
Bron & controle, plus Afkortingen and Parameters tabs on the right. The scenario year follows the valuation year plus the projection
period. Onderneming contains company identity and currency; Koers & winst contains
the split raw/derived price and earnings fields. Waardering & koopzone contains
fair/buy prices, safety margin and signals. Scenario contains growth, exit multiple,
projection period and bear/base/bull outcomes. Bron & controle contains source
metadata, review flags, risk notes and input warnings. All six shared controls
(safety margin, projection years and bear/bull growth/multiple adjustments) are
edited together in Parameters. Formula links to these inputs open that tab.
Their read-only reference columns remain in the financial views. Company stays fixed on the left, and search/sort choices
persist across panel switches without another source request.
The shared toolbar and tab strip stay mounted in the same positions for every tab.
Search/sort remain visible but disabled for Parameters and Afkortingen. All tab
contents use one fixed-height, internally scrolling viewport with a reserved
scrollbar gutter. Company columns and financial row/header heights stay consistent.
Formula details overlay this viewport and close when navigating to an input,
instead of expanding the page above the table. The previous
six-column table remains under **Historical reference**. Rows continue to come
from the selected portfolio's saved holdings. Portfolio membership codes use the
latest saved holdings across the dashboard, matched by exact security ISIN.
The Excel membership flags no longer populate this column.

**Afkortingen** explains every membership code, the profiles it covers, source,
holdings dates and coverage status. BB is a union of the four standard Bustelberg
profiles: present in any one profile is sufficient. BB2 is exclusively Bustelberg
2.0 Offensief and does not enter the BB union. TB similarly combines Toppenberg
profiles; IF means Imagine Fund. MTS/FTS/ETS/DTS/AI/STAR/ATS refer to Merken,
Familie, Europa, Dividend, AI, Star and Azie TopSelectie. MOM/OTS/VTS identify
Momentum, Obligatie and Vastgoed TopSelectie.

Core portfolio membership uses the saved linked AIRS model company holdings,
including available linked-certificate look-through. Topselecties membership uses
only saved Leonteq compositions. These reads never log in, refresh providers or
fall back to AIRS or the workbook. Missing profiles, unlinked models or opaque
certificate constituents make coverage incomplete, while known positive memberships
remain visible. An absent code is not a confirmed non-holding when coverage is
incomplete. Membership chips show full names and the matching profiles on hover.
Missing data is summarized once through the membership header's Coverage details
link and in Afkortingen, rather than repeated on every company row. Each displayed
code is a confirmed positive membership. A partially loaded family still has
confirmed membership when one profile holds the security; missing profiles cannot
undo that fact. Per-company unknown codes contain only incomplete families without
a confirmed match. If no code is confirmed and some families remain unknown,
the cell says Not confirmed instead of implying verified non-membership.
For multi-profile families, the dashboard roster defines which profiles are active.
An unlisted/retired profile does not create a coverage gap; a listed portfolio with
no linked holdings still does. An entirely missing family remains unavailable.

Stock prices remain saved Yahoo closes for the exact security. The workbook view
loads two consecutive future annual GuruFocus consensus EPS observations, preferring
EPS without NRI and retaining a single metric series. A missing forecast is not
replaced by historical EPS, a workbook estimate, or another listing's fundamentals.
The EPS basis and fiscal dates appear in the company editor. NTM EPS weights FY1
by the number of calendar months remaining (including the current month), with FY2
filling the other months. The analyst can override the FY1 month count.

The supplied file seeds growth, fair/exit multiples, review flags, risk notes and
the six scenario parameters for 104 companies. Seeds match an exact GuruFocus
ticker plus exchange; unqualified workbook tickers only match US securities.
Unmatched names require analyst input, never fuzzy name matching. Workbook market
prices and EPS forecasts are not imported as current data. Special NAV/book-value
inputs are retained as explicitly manual, dated source snapshots with their original
currency and URL. They are not relabelled as live GuruFocus data. Manual bases may
also represent FCF or SOTP, with an explicit label and source.

Click a company name to edit assumptions. **Scenario assumptions** edits the model
parameters. Inputs are saved per linked model portfolio in
`portfolio_valuation_assumptions`, independently of daily source refreshes. Source
prices and forecasts are never editable through this endpoint. Clearing an input
is preserved, including zero growth, instead of restoring its workbook default.

The calculations follow the workbook: fair price = basis × fair multiple;
buy multiple = fair multiple × (1 − safety margin); buy price = basis × buy multiple.
Both price-gap columns use target / current price − 1, which differs from the
historical reference's discount definition. Scenarios compound the chosen basis
at the assumed growth for the projection period, then apply the exit multiple and
the selected scenario adjustment. Base CAGR is price-only, excluding dividends.
The buy signal respects the workbook's manual review flag. Missing/nonpositive
bases or missing/stale Yahoo prices cannot generate a buy signal.

Raw Yahoo quotes are read without unit conversion; normalization is a separate
calculation in the workbook view.
Manual pence bases are normalized once. Different basis and quote currencies
require an explicit analyst conversion rate; an ADR ratio is never guessed.
Apply `20261116140000_valuation_workbook_assumptions.sql` before deploying this
view. Reads do not create saved assumptions or call Yahoo/GuruFocus; the normal
price and consensus ingestion jobs still own source acquisition.

The workbook uses blue for direct data, green for assumptions and yellow for
calculations, in both headings and cells. Raw Yahoo closes retain their original
quote units. Unit normalization is a separate yellow output, not a blue price
that has already been calculated. GuruFocus FY1/FY2 EPS, fiscal dates, currencies
and earnings measure are exposed as separate blue fields before the yellow NTM
blend. Manually transcribed NAV/BV/FCF facts, their date and source remain blue;
choosing that basis and its conversion rate is green. The six scenario controls
are also green. Display rounding and percentage formatting do not modify inputs.

Every yellow evaluator accepts only primitive source facts and assumptions.
Displayed yellow outputs are never passed into another evaluator; repeated
subexpressions expand from the blue/green inputs. Formula tooltips expose these
input-only expressions. Unit tests enforce both input dependencies and numerical
agreement with the supplied workbook, including pence quotes and zero overrides.
Clicking a yellow heading opens its formula and links to its blue/green inputs.
These links switch to the relevant panel and highlight the field, or open/focus
the shared scenario control. Changing panels does not alter formula evaluation;
the calculation engine always receives the complete set of primitive inputs.

## Historical reference

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

Taiwan Semiconductor's held ADR (`US8740391003`) uses the explicitly reviewed
Taiwan ordinary-share valuation reference (`TW0002330008`, GuruFocus `TPE:2330`).
Both Yahoo prices and GuruFocus EPS come from that same reference in TWD; these
are ordinary-share valuation figures, not USD ADR quotes. The earnings basis
label identifies this explicitly. Other ISIN aliases are not automatically
accepted for valuation. Membership and saved analyst overrides remain keyed to
the held ISIN.


The explicit Refresh button now processes each current company through
`POST /api/airs/valuation/portfolios/{id}/rows/{isin}/refresh`. It forces a
GuruFocus financials request, refreshes Yahoo prices through the company-price
adapter, and forces GuruFocus analyst estimates before recalculating the row.
Unchanged provider observations count as a successful refresh. Provider failures
retain the previous saved valuation and appear in the shared progress popup;
other rows continue. The response includes just that row's workbook inputs,
which the client merges without restarting the full workbook load or changing tabs.

Workbook reads obtain recent Yahoo closes independently of historical P/E
availability. Membership coverage is loaded separately after the core table so
cross-portfolio coverage checks cannot hold up quotes, forecasts or assumptions.
An initial input read has a 30-second timeout and an explicit retry control.
Scheduled and initial historical preparation remain database-only.


Assumption saves validate portfolio scope and write the settings, then return a
small typed acknowledgement containing the persisted assumptions and timestamp.
They do not rebuild the workbook, reread market data, or scan membership coverage.
The client merges the acknowledgement into its existing source facts and recalculates
locally only after the write succeeds, keeping the editor open if saving fails.

Refresh performance: mapped companies fetch Yahoo, GuruFocus financials and
current consensus concurrently within a row (at most three workers; the shared
provider rate limiter remains active). Rows still run sequentially. Financials
persist only the quarterly EPS codes used by historical valuation, and consensus
persists only the two annual EPS forecast series. The full fresh provider JSON
is retained for other screens. Valuation does not fetch the unrelated historical
analyst-estimate feed; other callers retain that default behavior. New company
resolution receives the same EPS scope and its financials fetch is not repeated.
The workbook row response reuses the already-authorized, freshly saved bundle.
Source-refresh elapsed time is logged for live timing; reduced metric volume is
not a claim of the same proportional reduction in end-to-end latency.


Reviewed GuruFocus listing overrides are declared in
`backend/config/gurufocus_listing_overrides.json`, with issuer identity and
GuruFocus listing sources. BESI's ordinary Amsterdam share (`NL0012866412`)
resolves to `XAMS:BESI` without relying on the provider's ISIN bridge. This does
not bypass exchange entitlements or financials fetches. An empty, successful
quarterly EPS import is not an upstream outage: historical P/E can remain
unavailable while Yahoo prices and annual consensus refresh normally. Actual
mapping failures, subscription restrictions and upstream errors retain distinct
messages; the popup receipt includes the failed company and its reason.

GuruFocus error receipts now carry reader-safe HTTP diagnostics through both
mapping ingestion and direct source refresh: empty/malformed HTTP 200 responses,
429 limits, 401 credentials, 403 access restrictions, 404 listing problems,
upstream 5xx failures and connection/timeout failures are distinguished without
including request URLs or API keys. Explicit valuation financials refreshes retry
an empty HTTP 200 once through the shared rate limiter; quota/access errors are
not retried by this loop. Empty statement/forecast templates never replace raw
cached data or advance the fetched timestamp. Persistent source failures remain
visible failures and retain the saved valuation.

Membership date presentation keeps two independent fields: Leonteq composition
`as_of` is the source holdings date; `retrieved_at` is when that successful snapshot
was fetched. Missing source dates display “Date unavailable”, never a retrieval
date substituted as a holdings date. Missing retrieval metadata displays “Not
recorded”. Coverage completeness describes identifiable holdings, not freshness.
The Topselecties snapshots were refreshed directly from Leonteq on 16 November
2026 and all 10 saved snapshots were reread successfully with both fields present:
AITopSelectie source date 15 November 2026; the other nine source dates 13 November
2026. No AIRS fallback was used.


Afkortingen offers Refresh holdings with AIRS + Leonteq, AIRS, and Leonteq scope.
It starts a cancellable `valuation.holdings.refresh` job via
`POST /api/airs/valuation/holdings/refresh/job?source=all|airs|leonteq`.
AIRS refreshes only the linked BB, BB2, TB and IF model compositions, under the
existing AIRS session lock. It never refreshes AIRS Topselectie compositions.
Leonteq refreshes its catalogue once and then each configured Topselectie
composition, without refreshing performance/accounting or using AIRS fallback.
The shared popup reports progress; finishing, failing or cancelling reloads
membership metadata so completed snapshots stay visible. A source failure is
reported as incomplete and cannot turn a cached source response into a fresh one.

All nine AIRS membership portfolios were refreshed on 16 November 2026; all ten
Leonteq snapshots remained unchanged during that AIRS-only operation. The AIRS
reader tried today's placeholder and retained the latest non-empty dated model:
BB 28 April 2025, BB2 23 July 2026, TB 8 June 2026 (Offensief 29 May 2026),
IF 14 October 2026. Those effective dates are not retrieval timestamps. The AIRS
`positions_scanned_at` field is now exposed separately as Last retrieved.

The valuation page now loads one combined `/portfolios/{id}/dashboard` payload
for every workbook tab. Tab changes make no requests.
Refresh uses one `/dashboard/refresh` POST stream for holdings refresh, source
updates, per-row progress and the final full payload; no polling or per-row HTTP
requests are needed. Topselecties use saved/refreshed Leonteq Share holdings only.

`portfolio_valuation_daily` now runs daily at 08:00 Europe/Amsterdam (DST-aware).
It refreshes the authoritative AIRS/Leonteq holdings, refreshes Yahoo/GuruFocus
inputs once per company across portfolios, and persists each complete workbook
payload. Ordinary selection reads the prepared payload and overlays current saved
analyst assumptions without rebuilding source data. Source failures retain saved
data and are recorded as incomplete. The backend scheduler must be running for
the registered daily trigger to execute.


With no portfolio selected, Refresh starts `POST /api/airs/valuation/prepare/refresh`.
This shares `valuation_dashboard.prepare_all` with `portfolio_valuation_daily`:
when the roster is empty or unlinked it discovers AIRS accounts and model
compositions, then refreshes every dashboard model and the configured Leonteq
Topselecties. It resolves missing company identities, fetches Yahoo history and
GuruFocus statements/forecasts, and saves complete workbook bundles, including
membership evidence. A migrated empty database needs configured provider
credentials and network access, but no manually populated source snapshots.
The returned roster populates the picker without another request. Portfolio
selection then uses its single bundled GET.

Preparation is exclusive across manual and daily runs. Cancellation and partial
failures retain completed writes; unresolved links or incomplete memberships
remain explicit failures. A selected portfolio refresh also fills missing
membership snapshots. The daily run covers all dashboard models, including
models outside the membership-code families, and shares fetched company inputs
across portfolios. No provider work runs on ordinary selection reads.
