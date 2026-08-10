import { periodDenoms, weightAt, type Weighted } from './marginData';

/**
 * The two extra LINES every ratio card's drill-down carries under its raw inputs: the market cap
 * that period, and the weight it produced.
 *
 * ⚠ WHY THESE ARE ROWS AND NOT A THIRD LINE INSIDE EACH CELL. The growth cards' drill-down
 * (`HoldingsRevenueModal`) stacks three numbers in one cell because it has ONE line per company.
 * These tables already put one line per ROW — Revenue, FCF, SBC, then the ratio — and name it in
 * the `Line` column. Cap and weight are simply two more of those, which is why this reads as part
 * of the table rather than as a second convention bolted onto it.
 *
 * ⚠⚠ THE WEIGHT IS THAT PERIOD'S, NOT TODAY'S — and the denominator comes from `periodDenoms`,
 * which applies the same two tests as the `weightedByYear` that drew the line. Weighting 2018 by
 * today's cap is look-ahead bias: measured on the S&P, NVIDIA is carried at 7.46% of a year it was
 * 0.63% of, and the FCF-SBC margin benchmark moves up to 3.00pp. The column sums to exactly 100%
 * in every period, which is what makes it checkable against the chart.
 *
 * ⚠ NO CAP ROW ON A PORTFOLIO. A holding weight is not a market cap and has no history, so a book
 * gets the weight line alone — its weight genuinely does not move between periods except as the
 * reporting set changes, which is itself worth seeing.
 */

/** EUR → a compact figure. Absolute euros in, like `market_cap_by_period`. */
function capBn(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '—';
  const a = Math.abs(v);
  if (a >= 1e12) return `${(v / 1e12).toFixed(2)}tn`;
  if (a >= 1e9) return `${(v / 1e9).toFixed(0)}bn`;
  return `${(v / 1e6).toFixed(0)}M`;
}

/** The blank identity cells a continuation row needs so its `Line` cell lands in the right column.
 *  ⚠ The first one stays sticky — without it the row slides under the pinned Company column when
 *  the table scrolls sideways, which is visible as a gap the other rows do not have. */
function Lead({ n }: { n: number }) {
  return (
    <>
      <td className="px-3 py-1 sticky left-0 bg-card z-10" />
      {Array.from({ length: Math.max(0, n - 1) }, (_, i) => <td key={i} />)}
    </>
  );
}

export { periodDenoms };

export function CapWeightLines<T extends Weighted>({ row, years, denoms, lead = 5 }: {
  row: T;
  years: string[];
  /** From `periodDenoms`, computed ONCE per table over all rows — never per row, which would be
   *  quadratic and, worse, would let two rows disagree about the same period's denominator. */
  denoms: Record<string, number>;
  /** How many identity columns precede `Line` (Company, GF exch, Ticker, Weight, Ccy = 5). */
  lead?: number;
}) {
  const caps = row.market_cap_by_period;
  return (
    <>
      {caps && (
        <tr className="hover:bg-overlay/[0.02]">
          <Lead n={lead} />
          <td className="px-3 py-1 whitespace-nowrap text-fg-dim">cap (EUR)</td>
          {years.map((y) => (
            <td key={y} className="px-3 py-1 text-right font-mono text-fg-dim">
              {capBn(caps[y])}
            </td>
          ))}
        </tr>
      )}
      <tr className="hover:bg-overlay/[0.02]">
        <Lead n={lead} />
        <td className="px-3 py-1 whitespace-nowrap text-fg-faint">weight</td>
        {years.map((y) => {
          const w = weightAt(row, y);
          const d = denoms[y];
          // ⚠ A DASH, NOT 0%, WHERE THE ROW IS NOT IN THIS PERIOD'S AVERAGE. Three ways that
          // happens — no cap that period, no weight at all, or the card's own inputs missing so
          // the ratio could not be computed — and all three mean "out of this period", which a
          // 0% would misreport as a holding too small to matter.
          const pct = w != null && d ? (100 * w) / d : null;
          return (
            <td key={y} className="px-3 py-1 text-right font-mono text-fg-faint">
              {pct == null ? '—' : `${pct.toFixed(2)}%`}
            </td>
          );
        })}
      </tr>
    </>
  );
}
