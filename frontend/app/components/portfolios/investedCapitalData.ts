/** Shared helpers for the Invested-capital card and its drill-down. Invested capital = non-current
 *  liabilities + total equity — the SAME base the Cash-return card divides FCF by, computed from
 *  the two raw lines the `cash-return-inputs` endpoint already returns (so no new endpoint, and the
 *  two cards can't disagree). A currency LEVEL, so it's plotted like Revenue (log axis), not as a
 *  ratio. */

import { type CashReturnRow } from './cashReturnData';
import { periodToX } from './marginData';
import { buildBlend, periodOrder, type Blend, type Resp, type Row } from './fundamentalBlend';

/** One company's invested capital per fiscal year (2015+), in its reporting currency. Both legs
 *  must be present for a year — a missing non-current-liabilities line (a bank / Berkshire) means
 *  the base is undefined, not "equity alone". */
export function investedCapitalSeries(row: CashReturnRow): Map<number, number> {
  const out = new Map<number, number>();
  const years = new Set<string>([...Object.keys(row.noncurrent_liabilities), ...Object.keys(row.total_equity)]);
  for (const y of years) {
    // ⚠ `periodToX`, NOT `Number` — a "2025-Q3" label is NaN to `Number`, and every quarterly
    // period would land on one NaN key. Compared as a STRING for the 2015 floor, because the
    // labels sort lexically either way and parsing to compare would reintroduce the same trap.
    if (y < '2015') continue;
    const ncl = row.noncurrent_liabilities[y];
    const eq = row.total_equity[y];
    if (ncl == null || eq == null) continue;
    out.set(periodToX(y), ncl + eq);
  }
  return out;
}

/**
 * The same series with its period LABELS kept, ordered along the axis.
 *
 * ⚠ `weightAt` IS KEYED ON THE LABEL THE SERVER SENT, and `periodToX` does not invert — 2025.25 is
 * "2025-Q2" only if you already know the cadence. Rebuilding a label with `String(x)` (what this
 * function replaced) yields "2025.25", which no `market_cap_by_period` key matches, so every
 * quarterly cap lookup silently fell through to the as-of scan and resolved by STRING comparison
 * against "2025-Q2". Harmless while the caps were being dropped anyway; a live bug the moment they
 * are carried through, which is the whole point of the fix below.
 */
function labelledSeries(row: CashReturnRow): { label: string; value: number }[] {
  const out: { label: string; value: number }[] = [];
  const years = new Set<string>([...Object.keys(row.noncurrent_liabilities), ...Object.keys(row.total_equity)]);
  for (const y of years) {
    if (y < '2015') continue;
    const ncl = row.noncurrent_liabilities[y];
    const eq = row.total_equity[y];
    if (ncl == null || eq == null) continue;
    out.push({ label: y, value: ncl + eq });
  }
  return out.sort((a, b) => periodToX(a.label) - periodToX(b.label));
}

/**
 * A portfolio's or an index's invested capital as a GROWTH INDEX.
 *
 * ⚠⚠ IT GOES THROUGH `buildBlend` — THE ONE CHAINED RULE — AND USED TO AVERAGE REBASED LEVELS
 * INSTEAD, WHICH WAS WORTH 7.3 POINTS A YEAR (2026-08-21). Every other level line on this tab is
 * chained from weighted GROWTH between consecutive drawn periods; this one rebased each member to
 * 100 at its own base and took a cap-weighted average of those levels. Two defects came with that,
 * and they compound:
 *
 *   1. THE LEVEL `v(y)/v(base)` IS A GROWTH RATIO AND THE CAP AT `y` ALREADY CONTAINS IT — the same
 *      circularity `blend_series` carried until the same day. A constituent whose capital base grew
 *      is generally one whose market cap grew, so it was over-weighted in its own growth.
 *   2. AVERAGING REBASED LEVELS MAKES THE LINE AN ARTEFACT OF WHEN EACH MEMBER'S HISTORY STARTS.
 *      Everyone is 100 at their own base, so a constituent entering the panel drags the average
 *      toward 100 and the index "moves" on composition alone. `_fundamental_blend` records that one
 *      drawing a 388 → 285 crash into 2023 that no constituent experienced, which is why the
 *      backend abandoned this construction — this card was the last place still using it.
 *
 * Measured on ACWI, 1,283 constituents with both legs, 2015→2025
 * (`backend/scripts/profile_price_index_weighting.py --metric invested_capital --rebased-avg`):
 * rebased-level average **+18.14%/yr** against the chain's **+10.81%/yr**.
 *
 * ⚠⚠ THE VERTIV GUARD DID NOT VANISH, IT MOVED SOMEWHERE STRICTER — and this is the part worth
 * reading before touching it. The old code chose each member's base as the first period it could be
 * WEIGHTED in, because Vertiv listed via SPAC in Feb 2020 and its pre-2020 fiscal years under `VRT`
 * are the blank-cheque SHELL: invested capital of **0.024M** (founder capital) in 2017, 696.1M in
 * 2018 once the IPO trust landed, 3,332.2M in 2020 when the business arrived. Three entities in one
 * column. `base > 0` waved 0.024 through at an index of 2,784,248 in 2018 and 31,221,600 by 2025 —
 * one row took the S&P 500 line to 33,849 where the honest figure is ~561.
 *
 * In a CHAIN the base is irrelevant: `g = at(y)/at(anchor) − 1` divides it out, and `memberScale`
 * is a ratio against the member's own median, so both are scale-free. What catches Vertiv now is
 * `stepGrowth`, twice over: its 2018 figure is 29,000× its 2017 one (over `_MAX_STEP_GROWTH`, 100×)
 * AND that 0.024 base is 0.00002 of its own median (under `_MIN_STEP_BASE_FRACTION`, 0.10). It sits
 * out those steps and rejoins, rather than being re-based around. ⚠ Those two guards are general —
 * they catch every IPO, spin-off and redenomination of the same shape (VICI, Carvana, CrowdStrike)
 * — where the base rule only worked where a per-period cap happened to exist, and was explicitly
 * inert for a portfolio holding. The chain protects both.
 *
 * ⚠ `buildBlend` IS THE CLIENT TWIN OF `_fundamental_blend.blend_series`, so this line is now
 * computed by the same rule as the server computes Revenue, EPS and the share price — including the
 * anchor-weighted step, the carry-forward and both coverage floors. There is no invested-capital
 * blend left to drift.
 */
/**
 * The blend itself, for callers that need more than a `{year: value}` map.
 *
 * ⚠ EXTRACTED SO THERE IS STILL ONE CONSTRUCTION. The `Tables` tab's invested-capital CAGR row
 * needs the `Blend` (its `level` keys are PERIODS — `lineCagr` reads `LTM` and `2026e`, which a
 * `Map<number, …>` has already thrown away), while the card wants the year map. Building a second
 * payload there "the same way" is exactly what this file's header warns about, so the map is now
 * derived from this rather than beside it.
 */
export function investedCapitalBlend(rows: CashReturnRow[]): Blend {
  // ⚠ `revenue` IS `buildBlend`'S FIELD NAME FOR "THE SERIES", not a claim about revenue — the
  // payload shape it takes is the drill-down matrix's, whose metric column is named that whatever
  // the metric is (see `Row`). Mapping into it is what buys the one implementation.
  const asRows: Row[] = rows.map((r) => ({
    isin: r.isin,
    name: r.name,
    weight_pct: r.weight_pct,
    currency: r.currency,
    ticker: r.ticker,
    exchange: r.exchange,
    status: r.status,
    // ⚠ THE PER-PERIOD CAPS MUST TRAVEL. Without them `wAt` falls back to one constant share across
    // a decade — the look-ahead bias every other card here avoids, and the reason a zero-cap shell
    // period is excluded from the average at all.
    market_cap_by_period: r.market_cap_by_period,
    revenue: Object.fromEntries(labelledSeries(r).map((p) => [p.label, p.value])),
  }));
  // ⚠ EVERY row stays in the list, including the ones with no series — they are the denominator the
  // coverage floor is measured against. Filtering them out first would make a year computed over
  // two of twelve holdings read as 100% covered.
  const years = [...new Set(asRows.flatMap((r) => Object.keys(r.revenue)))].sort(periodOrder);
  const resp: Resp = { years, rows: asRows, holdings: asRows.length };
  return buildBlend(resp);
}

export function investedCapitalIndexByYear(rows: CashReturnRow[]): Map<number, number> {
  const out = new Map<number, number>();
  for (const [label, point] of Object.entries(investedCapitalBlend(rows).level)) {
    out.set(periodToX(label), point.value);
  }
  return out;
}
