# TopSelecties source policy

The management dashboard uses the same `PortfolioOverviewPanel` for every collection,
including the same table, sorting, allocation-policy control, Refresh all and Analyse actions.
TopSelecties reads exclusively from Leonteq LYNQS through
`/api/leonteq/management/topselecties/dashboard`. The other collections retain their AIRS path.
Opening the tab reads saved snapshots only. Refresh runs a background job that reads the
Leonteq catalogue, composition and monthly performance; it never refreshes AIRS.

`backend/config/management_topselecties.json` lists the ten products and their Leonteq
identities. Matching accepts normalized exact names (including diacritics), including the explicit name without
the trailing `Index`. The configured canonical Leonteq name takes precedence over a
display-name alias matching a different product: Star Selection Index is distinct from
StarTopSelectie Index. Alias-only and duplicate canonical matches still fail closed when
ambiguous. Missing or ambiguous matches are unavailable; no fuzzy name matching
or AIRS data fallback is permitted. Verify these identities against the live catalogue on
first deployment. Existing snapshots remain scoped to the configured LYNQS account.

The table retains Name, Month-to-date, Year-to-date, Since and To. MTD comes from Leonteq's
current-month performance; YTD comes from its catalogue. Since is the later of inception and
the start of the return year. To uses only an explicit Leonteq portfolio valuation date;
retrieval dates and individual holding price dates never substitute for that date. Older
snapshots without an explicit valuation date show a dash until refreshed. Source tooltips
identify Leonteq. Position AVG performance is never substituted for portfolio YTD.

Analyse reads `/api/leonteq/management/topselecties/{name}/analysis`, retaining cash, source
position values and current weights in the shared valued-holdings table. It uses Leonteq's
reported YTD, never the generic basket reconstruction. Its benchmark is measured over the
same start/end dates, using Leonteq's actual index-performance history rather than the
commercial issue date or a separate calendar-year fallback. This lightweight return-window
read does not require opening holdings or a successful accounting ledger. The source index
marks must reproduce Leonteq's published YTD. ACWI is then priced in EUR using the existing
Yahoo benchmark service over precisely those requested boundaries, as for AIRS portfolios.
The window is cached separately inside the existing performance snapshot without restamping
older monthly observations. If source return dates cannot be established, no unrelated
calendar-YTD number or excess is substituted. Opening-position gaps affect contribution,
not the ability to obtain and align the source index return window.
The position-return column can display Leonteq's source AVG Performance before an accounting
ledger is available. Its tooltip identifies AVG Performance and its unspecified period,
without claiming an EUR conversion; it is not treated as portfolio YTD or used to compute contribution.
A reconciled ledger replaces that figure with the explicitly described period cash-flow return.
Risk classifications
and instrument risk metrics use the existing market-data services. Fundamental and earnings
workspaces receive Leonteq ISIN holdings and weights without an AIRS account/model ID.
An explicit refresh also reads the complete paged index-transactions feed, including
Buy, Sell, Dividend and AMC Fees events, signed gross/net amounts, booking currency,
booking FX and source valuation FX. Net cash uses the source net amount and booking FX;
dividend withholding and trading charges are not subtracted again. Unknown events and
non-EUR booking cash remain explicit gaps, never gross-amount or AIRS fallbacks.

The reader requests dated compositions for the opening return mark and each following
valuation date, using the portal's confirmed `composition?date=YYYY-MM-DD` endpoint.
It stores source evidence and the latest acquisition result in `leonteq_accounting_snapshot`.
Each interval must reconcile source quantities, cash, NAV and reported index performance.
Capital inflows/outflows require a source-return-consistent beginning/end timing basis;
ambiguous timing, splits/transfers, missing net cash/FX or missing historical boundaries
withhold the calculation. Daily contributions are geometrically linked and checked against
Leonteq YTD before publication. Sold-out positions and separately identified AMC fees remain
in the total. The position-return column uses net period profit divided by opening value plus
net purchases, explicitly described as a cash-flow capital return rather than a TWR.

The table, columns and controls are unchanged. Existing contribution tooltips explain source
gaps. A successful old ledger cannot be blended into newer source holdings or returns.
The Performance button is available beside Risk for Leonteq books as well. It opens the
same monthly report panel, populated from saved Leonteq monthly returns and the aligned
YTD comparison. Historical per-month benchmark and position attribution remain unavailable
until separately calculated; this panel never loads an AIRS bundle or starts AIRS jobs.
For AITopSelectie, source acquisition on 16 November 2026 found the first return mark on
8 June 2026 but no composition for that date. Transactions has 19 initial buys, whereas
Instructions has 20 BOOKED initial buys, including 107 Alphabet shares missing from
Transactions. Its instruction detail and audit log confirm execution and booking.
All 20 quantities match the 9 June composition. The reader can reconstruct an all-cash
inception by reversing these net EUR execution amounts out of that composition's cash.
The resulting opening is about EUR780,000 and independently reproduces the first index
return. It does not solve opening NAV backwards from the published return.
Instructions are paginated, only BOOKED fills count, source RIC resolves a missing ISIN,
and matching transaction fills are counted once. Conflicting amounts, residual initial
holdings or a first-return mismatch withhold attribution. The reconstructed source evidence
and method are retained in the snapshot. This recovery is limited to the actual initial
index boundary, never a missing year-opening balance of an existing book.
Negative EUR cash remains a cash balance; short securities are unsupported.
The full 115-date audit still fails on 9–10 June: EUR cash increases by 1,002.90
with no transaction on 10 June, and neither start- nor end-of-day funding treatment
reproduces the index return from the composition P&L. The instructions feed has no
10 June order either. Opening reconstruction therefore removes one genuine gap but
does not make the full YTD contribution publishable. The accounting failure includes
the exact interval; no residual is silently assigned to a security or labelled as fees.

Certificate look-through in other portfolios uses Leonteq TopSelecties constituents. The
parent portfolio's own certificate valuation remains AIRS. Missing constituents leave the
wrapper opaque. AIRS child valuations, ledgers and historical monthly reports are blocked
for TopSelecties. Current Leonteq weights are never substituted for historical sale weights.

Apply `20261116120000_leonteq_management_source_revision.sql` and
`20261116130000_leonteq_accounting_snapshots.sql` with the normal database
migration process before deploying. It invalidates prepared holdings/risk on Leonteq snapshot
writes. Source-policy code and identity configuration participate in prepared-code versions.
Use **Refresh all** on the TopSelecties tab after deployment to populate missing snapshots.

Validation uses fake storage and mocked vendor responses. No live provider or database
requests are part of the test suite.
