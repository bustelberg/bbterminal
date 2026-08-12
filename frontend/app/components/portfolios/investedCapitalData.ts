/** Shared helpers for the Invested-capital card and its drill-down. Invested capital = non-current
 *  liabilities + total equity — the SAME base the Cash-return card divides FCF by, computed from
 *  the two raw lines the `cash-return-inputs` endpoint already returns (so no new endpoint, and the
 *  two cards can't disagree). A currency LEVEL, so it's plotted like Revenue (log axis), not as a
 *  ratio. */

import { type CashReturnRow } from './cashReturnData';
import { periodToX, weightAt, weightedByYear } from './marginData';

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
 * A portfolio's invested capital as a GROWTH INDEX — each company's series rebased to 100, then
 * weight-averaged per period (mixed-currency levels can't be summed; growth can be blended, exactly
 * as Revenue is for a portfolio).
 *
 * ⚠⚠ THE BASE IS THE FIRST PERIOD THE COMPANY CAN BE **WEIGHTED** IN, NOT THE FIRST PERIOD IT HAS A
 * FIGURE FOR — and `base > 0` is nowhere near enough on its own. Vertiv listed via SPAC in Feb
 * 2020, so GuruFocus's pre-2020 fiscal years under `VRT` are the blank-cheque SHELL: invested
 * capital of **0.024M** (founder capital) in 2017 against 696.1M in 2018 once the IPO trust landed,
 * and 3,332.2M in 2020 when the actual business arrived. Three different entities in one column.
 * 0.024 is positive, so the old guard waved it through at an index of 2,784,248 in 2018 and
 * 31,221,600 by 2025 — measured, that ONE row took the S&P 500 line to **33,849** in 2025 where the
 * honest figure is ~561, and produced the entire 2017→2018 "skyrocket" (273.2 drawn vs 151.6 real).
 * VICI Properties (base 90.8, pre-Caesars-spin-off → 50,654), Carvana and CrowdStrike are the same
 * shape from IPOs and spin-offs.
 *
 * The rule is structural rather than a threshold or a blacklist, and it already exists elsewhere in
 * this file's neighbours: a period with no usable market cap is one the company is EXCLUDED from
 * the average in (see `weightAt`), so it has no business serving as the base of the index it is
 * averaged into. Vertiv's cap is 0 in 2016 and 2017 and real from 2018, so it now bases at 2018.
 * Nothing to tune and nothing to maintain — a threshold would need revisiting for every new SPAC.
 *
 * ⚠ INERT FOR A PORTFOLIO, DELIBERATELY. A holding weight has no history (`weightAt` returns the
 * same `weight_pct` for every period), so "first weightable period" is "first period with a figure"
 * there and the behaviour is unchanged. We have nothing that says when a name became investable, so
 * this refuses to invent it — a portfolio holding Vertiv still shows the shell's base.
 */
export function investedCapitalIndexByYear(rows: CashReturnRow[]): Map<number, number> {
  const rebased = rows.map((r) => {
    const pts = labelledSeries(r).filter((p) => weightAt(r, p.label) != null);
    const idx = new Map<string, number>();
    const base = pts[0]?.value;
    if (base != null && base > 0) for (const p of pts) idx.set(p.label, 100 * p.value / base);
    // ⚠ THE PER-PERIOD CAPS MUST TRAVEL WITH THE REBASED ROW. Dropping them (what this did until
    // 2026-08-12) leaves `weightAt` with only the stable `weight_pct`, so an INDEX was weighted by
    // one constant share across a decade — the look-ahead bias every other card on this tab avoids
    // — and there was no per-period weight for the base rule above to read.
    return { weight_pct: r.weight_pct, market_cap_by_period: r.market_cap_by_period, idx };
  });
  // ⚠ EVERY holding stays in the list, including the ones with no series — they are the
  // denominator the coverage floor is measured against. Filtering them out first would make a
  // year computed over two of twelve holdings read as 100% covered.
  return weightedByYear(rebased, (r) => [...r.idx.keys()], (r, y) => r.idx.get(y) ?? null);
}
