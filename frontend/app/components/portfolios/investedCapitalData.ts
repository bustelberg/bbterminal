/** Shared helpers for the Invested-capital card and its drill-down. Invested capital = non-current
 *  liabilities + total equity — the SAME base the Cash-return card divides FCF by, computed from
 *  the two raw lines the `cash-return-inputs` endpoint already returns (so no new endpoint, and the
 *  two cards can't disagree). A currency LEVEL, so it's plotted like Revenue (log axis), not as a
 *  ratio. */

import { type CashReturnRow } from './cashReturnData';
import { weightedByYear } from './marginData';

/** One company's invested capital per fiscal year (2015+), in its reporting currency. Both legs
 *  must be present for a year — a missing non-current-liabilities line (a bank / Berkshire) means
 *  the base is undefined, not "equity alone". */
export function investedCapitalSeries(row: CashReturnRow): Map<number, number> {
  const out = new Map<number, number>();
  const years = new Set<string>([...Object.keys(row.noncurrent_liabilities), ...Object.keys(row.total_equity)]);
  for (const y of years) {
    if (Number(y) < 2015) continue;
    const ncl = row.noncurrent_liabilities[y];
    const eq = row.total_equity[y];
    if (ncl == null || eq == null) continue;
    out.set(Number(y), ncl + eq);
  }
  return out;
}

/** A portfolio's invested capital as a GROWTH INDEX — each company's series rebased to 100 at its
 *  first year, then weight-averaged per year (mixed-currency levels can't be summed; growth can be
 *  blended, exactly as Revenue is for a portfolio). */
export function investedCapitalIndexByYear(rows: CashReturnRow[]): Map<number, number> {
  const rebased = rows.map((r) => {
    const s = investedCapitalSeries(r);
    if (!s.size) return { weight_pct: r.weight_pct, idx: new Map<number, number>() };
    const y0 = Math.min(...s.keys());
    const base = s.get(y0)!;
    const idx = new Map<number, number>();
    if (base > 0) for (const [y, v] of s) idx.set(y, (v / base) * 100);
    return { weight_pct: r.weight_pct, idx };
  });
  // ⚠ EVERY holding stays in the list, including the ones with no series — they are the
  // denominator the coverage floor is measured against. Filtering them out first would make a
  // year computed over two of twelve holdings read as 100% covered.
  return weightedByYear(rebased,
    (r) => [...r.idx.keys()].map(String),
    (r, y) => r.idx.get(Number(y)) ?? null);
}
