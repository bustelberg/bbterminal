'use client';

import { type BASIS } from './quickValuation';
import { type AlignedRow, type TrailingPoint } from './multiplesSeries';

/**
 * The data behind the multiple-through-time chart: the close and the per-share figure it was
 * divided by, then the multiple that division produced.
 *
 * ⚠ IT IS HANDED THE PLOTTED ROWS, NOT AN ISIN — the same rule as `QuickValuationInputsModal`. The
 * `trail`/`fwd` columns are read straight off the exact rows the chart draws, and the two inputs
 * ride along on the trailing points (`TrailingPoint`), so the table cannot arrive at a different
 * multiple than the line it was opened from.
 *
 * ⚠ DATE PER ROW, NOT PER COLUMN — deliberately the TRANSPOSE of the fiscal-year drill-down beside
 * it. That one has ten columns and four lines, so lines-as-rows fits on a screen. This series is
 * weekly for a decade — ~550 observations — which as columns is a horizontal scroll nobody can
 * read. Same panel, different shape, because the data has a different shape.
 *
 * ⚠ NEWEST FIRST, which is also the opposite of the sibling. A reader arrives here from today's
 * number wanting to know what it was computed from; making them scroll a decade to reach it would
 * be a strange greeting. The chart is still drawn oldest-left — this is a table, not the chart.
 */

/** Matches `MultipleHistoryChart`: a multiple this far above the median is a collapsed denominator
 *  rather than a valuation, and those are exactly the rows a reader comes here to find. */
const OUTLIER_MULT = 5;

export default function MultipleHistoryModal({
  rows, trailing, basis: b, median, currency, name, isin, hasForward, fromYear, onClose,
}: {
  /** The aligned rows the chart plots — `t`, `trail`, `fwd`. */
  rows: AlignedRow[];
  /** The trailing observations, carrying the close and per-share each was computed from. */
  trailing: TrailingPoint[];
  basis: (typeof BASIS)[keyof typeof BASIS];
  median: number | null;
  currency?: string | null;
  name?: string | null;
  isin: string;
  hasForward: boolean;
  fromYear: number;
  onClose: () => void;
}) {
  // ⚠ KEYED ON THE TIMESTAMP THE CHART USES, so a row's inputs belong to the point above them. A
  // row with no entry is a timestamp contributed by the OTHER series, where this line is carried
  // rather than re-observed — see `align`. It gets a blank, never the previous row's close.
  const inputs = new Map(trailing.map((p) => [p.t, p]));
  const ceiling = median == null ? null : median * OUTLIER_MULT;

  const ordered = [...rows].sort((a, b2) => b2.t - a.t);
  const num = (v: number | null | undefined, d = 2) => (v == null ? '—' : v.toFixed(d));
  const mult = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(1)}×`);
  const day = (t: number) => new Date(t).toISOString().slice(0, 10);

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[88vw] h-[84vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">
            {b.multiple} — {hasForward ? 'forward & trailing' : 'trailing'}, by date
          </h2>
          {name && <span className="text-sm text-fg-soft truncate max-w-[28ch]" title={name}>{name}</span>}
          <span className="text-[11px] font-mono text-fg-faint">{isin}</span>
          <button type="button" onClick={onClose}
            className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-3">
          <p className="text-[11px] text-fg-faint">
            Trailing {b.multiple} is the close divided by the {b.perShare} last PUBLISHED at that
            date — the reporting lag is already applied, so no row uses a figure the market did not
            yet have. {hasForward
              ? `Forward ${b.multiple} is GuruFocus's own published indicator, read straight through and computed from nothing here.`
              : 'There is no forward line on this basis — nobody forecasts capex, so no vendor publishes a free-cash-flow consensus at any date.'}
            {' '}Weekly since {fromYear}, newest first · {ordered.length} rows · Source: {b.source}
          </p>

          <div className="overflow-auto rounded-lg border border-neutral-800/40">
            <table className="w-full text-xs">
              <thead className="bg-page sticky top-0 z-10">
                <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                  <th className="px-3 py-1.5 font-medium text-left">Date</th>
                  <th className="px-3 py-1.5 font-medium text-right">
                    Close{currency ? ` (${currency})` : ''}
                  </th>
                  <th className="px-3 py-1.5 font-medium text-right">
                    {b.perShare}{currency ? ` (${currency})` : ''}
                  </th>
                  <th className="px-3 py-1.5 font-medium text-right">Trailing {b.multiple}</th>
                  {hasForward && (
                    <th className="px-3 py-1.5 font-medium text-right">Forward {b.multiple}</th>
                  )}
                </tr>
              </thead>
              <tbody>
                {ordered.map((r) => {
                  const inp = inputs.get(r.t);
                  // The rows the chart footer counts as "off the top of the axis". They are the
                  // reason to open this table at all, so they are findable in it.
                  const over = ceiling != null && r.trail != null && r.trail > ceiling;
                  return (
                    <tr key={r.t} className="border-t border-neutral-800/40 hover:bg-overlay/[0.02]">
                      <td className="px-3 py-1 whitespace-nowrap font-mono text-fg-soft">{day(r.t)}</td>
                      <td className="px-3 py-1 text-right font-mono text-fg-soft">{num(inp?.price)}</td>
                      <td className="px-3 py-1 text-right font-mono text-fg-soft">{num(inp?.perShare)}</td>
                      {/* ⚠ TEXT WEARS TEXT TOKENS, NEVER THE SERIES COLOUR — the house dataviz
                          rule. The amber here is the warning ramp, not the chart's amber line. */}
                      <td className={`px-3 py-1 text-right font-mono font-medium ${over ? 'text-warn-300' : 'text-fg-soft'}`}
                        title={over ? `Above ${OUTLIER_MULT}× the median (${mult(median)}) — a collapsed denominator, not a valuation. Drawn, but off the top of the chart's axis.` : undefined}>
                        {over ? '⚠ ' : ''}{mult(r.trail)}
                      </td>
                      {hasForward && (
                        <td className="px-3 py-1 text-right font-mono text-fg-soft">{mult(r.fwd)}</td>
                      )}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {hasForward && (
            <p className="text-[11px] text-fg-faint">
              ⚠ A blank Close and {b.perShare} is not a missing price. That row&apos;s timestamp comes
              from the forward series, which is sampled on its own dates; the trailing line is
              carried across it rather than re-observed, exactly as the chart draws it.
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
