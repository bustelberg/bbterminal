# /earnings — Banded quality charts (spec / WIP)

Status: **spec only, not implemented.** Picks up the request: "make charts out of the
snapshot stats with green/orange/red bands; missing data points can be skipped."

## Goal

For each snapshot stat that has a real time series, render a small line chart of that
metric over time with **green / orange / red horizontal background bands** showing the
good / warning / bad zones. The band cutoffs are the *same thresholds* the existing
table already scores against (`higherBetter` / `lowerBetter` in `SnapshotStats.tsx`).
Missing / N/A points are skipped (gaps in the line), not zero-filled.

## Decisions (locked)

- **Placement:** a **new "Charts" section *below* the existing Snapshot Stats table**
  (a sibling section, both visible). Do NOT replace or toggle the table.
- **Coverage:** OPEN — see "Open questions". Default for v1: **chartable direct-metric
  ratios only**; derived single-value stats (CAGR / R² / SD) stay table-only.

## Where the data + thresholds come from

- **Series:** `timeSeries(metrics, code)` (`app/components/earnings/utils.ts:60`) returns
  `{date, value}[]` sorted by date for a metric code. Use the **resolved** code — the
  snapshot prefers the `quarterly__…` twin over `annuals__…` (`useSnapshot.resolvedCode`),
  so charts should chart the same resolved code for freshness/consistency.
- **Thresholds:** currently inline in each `RowSpec.cellFrom` via:
  - `higherBetter(v, { poorBelow: P, goodAtOrAbove: G })` → **red** `v < P`, **orange**
    `P ≤ v < G`, **green** `v ≥ G`.
  - `lowerBetter(v, { goodAtOrBelow: G, poorAbove: P })` → **green** `v ≤ G`, **orange**
    `G < v ≤ P`, **red** `v > P`.
- **Units:** the stored series values are in the SAME units as the thresholds, so no
  conversion needed for the chartable set (percent-point metrics store `15` = 15%;
  ratio metrics store the raw ratio). CAGR/R²/SD are fractions (`0.12`) — but those are
  derived, not direct series (excluded from v1).
- Codes live in `app/components/earnings/types.ts` `MC` map.

## Stat catalog

### Chartable in v1 (direct single-code series + numeric thresholds)

| Stat | `MC` code | Direction | Bands (poor / good) | Unit |
|---|---|---|---|---|
| Interest Coverage | `INTEREST_COVERAGE` (GF series) | higherBetter | <3 / ≥7 | ratio |
| Debt / Equity | `DEBT_TO_EQUITY` | lowerBetter | >2 / ≤0.5 | ratio |
| CAPEX / Revenue | `CAPEX_TO_REV` | lowerBetter | >15 / ≤5 | % |
| CAPEX / OCF | `CAPEX_TO_OCF` | lowerBetter | >60 / ≤30 | % |
| ROE | `ROE` | higherBetter | <8 / ≥15 | % |
| ROIC | `ROIC` | higherBetter | <8 / ≥15 | % |
| Gross Margin | `GROSS_MARGIN` | higherBetter | <20 / ≥40 | % |
| Net Margin | `NET_MARGIN` | higherBetter | <5 / ≥15 | % |
| EPS LT Growth EST | `EPS_EST` | higherBetter | <5 / ≥12 | % |
| Forward P/E | `FWD_PE` | lowerBetter | >25 / ≤15 | ratio |
| PEG | `PEG` | lowerBetter | >2 / ≤1 | ratio (treat stored `0` as null — GF sentinel) |

Notes:
- **Interest Coverage**: the table *computes* it from Operating Income ÷ |Interest
  Expense| when both raw fields exist, else falls back to GF's `INTEREST_COVERAGE`
  series. For the chart, simplest v1 = plot the GF `INTEREST_COVERAGE` series when
  present; optionally compute per-period from aligned `OPERATING_INCOME` / `INTEREST_EXPENSE`
  (matches the table's freshness). Skip if neither available.

### Optional / v1.5 (computed per-period from two aligned series)

| Stat | Derivation | Direction | Bands |
|---|---|---|---|
| FCF / Net Income | `FCF` ÷ `NET_INCOME` aligned per period | higherBetter | <0.8 / ≥1.2 |

### Not chartable (single trailing-window derivation — leave table-only)

Price 5Y/10Y CAGR, Price 5Y/10Y R², Revenue 5Y Growth, Revenue R², FCF 5Y Growth,
FCF Growth R², FCF Growth SD. These are one number over a trailing window, not a series.
(If a trend view is wanted, the *underlying* price/revenue/FCF series already have
dedicated charts: `FCFShareChart`, `RelativeGrowthChart`, `ForwardPEChart`.)

## Band rendering semantics

Given a chart with y-domain `[ymin, ymax]` (see scaling below) and a direction + bounds:

- **higherBetter (P=poorBelow, G=goodAtOrAbove):**
  - red band: `[ymin, P)`
  - orange band: `[P, G)`
  - green band: `[G, ymax]`
- **lowerBetter (G=goodAtOrBelow, P=poorAbove):**
  - green band: `[ymin, G]`
  - orange band: `(G, P]`
  - red band: `(P, ymax]`

Bands are full-width horizontal rectangles behind the line, low opacity
(`color-mix(in srgb, var(--color-pos-500) ~12%, transparent)` etc., matching the
heatmap/`DailyReturnsHistograms` style). Use the theme tokens:
`--color-pos-500` (green), `--color-warn-500` (orange), `--color-neg-500` (red).

**Y-domain scaling:** include both the data range AND the two thresholds so the bands
are always visible even when the metric sits entirely in one zone. e.g.
`ymin = min(min(values), P, G)` with a small pad, `ymax = max(max(values), P, G)` padded.
Clamp pathological outliers if needed (some ratios spike).

## Chart component

- Pure SVG, no recharts (match `DailyReturnsHistograms` / the rolling-corr chart):
  `viewBox` + `preserveAspectRatio="none"`, `vectorEffect="non-scaling-stroke"` on the
  line, x-axis labels as HTML below the svg (not in the svg, to avoid stretch).
- **Skip missing points:** filter `value == null` / non-finite; break the line on gaps
  rather than interpolating (same `started`-flag pattern as `RollingCorr.linePath`).
- Hover: snap-to-nearest-point guide + tooltip (date + value + which band it's in) —
  reuse the interaction pattern from `RollingCorr` in `DailyReturnsHistograms.tsx`.
- Header: stat label + the existing `CellInfoTip` text + current value colored by its
  band; small legend isn't needed (bands are self-evident).

## UX / layout

- New section **below** `SnapshotStats`'s table, e.g. a `CollapsibleCard` titled
  **"Quality charts"** (default expanded on /earnings). Responsive grid
  (`grid-cols-1 sm:grid-cols-2 lg:grid-cols-3`), one mini chart per chartable stat,
  grouped by the same sections (Balance Sheet, Profitability, …) if convenient.
- **Comparison company (`metricsB`):** v1 = chart company A only. v2 = overlay B as a
  second line (no bands change — bands are per-metric, shared). Decide at build time.

## Implementation steps

1. Extract the chartable specs into a shared list (code, label, direction, bounds,
   unit, info) — ideally derived from the existing `RowSpec`s to avoid duplicating
   thresholds. Cleanest: add optional `chart?: { code: string; bounds: {good,poor};
   direction: 'higher'|'lower' }` to `RowSpec` and read it; or a parallel `CHART_SPECS`
   array in a new `earnings/qualityCharts.ts`. (Parallel array is lower-risk but
   duplicates the cutoffs — keep them in sync.)
2. New `app/components/earnings/QualityChartsSection.tsx` (+ a `BandedChart.tsx` mini
   chart). Pull `timeSeries(metrics, resolvedCode(code))` per spec; render bands + line.
3. Mount it in `EarningsDashboard.tsx` directly under `<SnapshotStats … />`.
4. Verify: `npx tsc --noEmit` + `npx eslint app/components/earnings/`.

## Edge cases

- Quarterly vs annual cadence — chart whatever `resolvedCode` resolves to; label the
  cadence (the snapshot already has `cadenceFor`).
- `%`-unit vs `ratio`-unit axis formatting (reuse `fmtPct`/`fmtNum` from `utils.ts`).
- PEG: stored `0` is GF's "undefined" sentinel → treat as null (skip point).
- Single-point series → render the dot on its band, no line.
- Bands with only one threshold meaningful (none currently — all use two cutoffs).

## Open questions (resolve before/at build)

1. **Coverage:** chartable ratios only (default), or also show derived single-value
   stats as a lone dot vs their bands? (User leaned toward "skip missing".)
2. Include the computed **FCF / Net Income** per-period chart in v1 or defer?
3. Overlay the **comparison company** (B) line in v1 or A-only?
4. One big "Quality charts" card, or per-section sub-cards mirroring the table layout?
