'use client';

import { type BASIS } from './quickValuation';
import { type AlignedRow } from './multiplesSeries';

/**
 * The data behind the multiple-through-time chart: the vendor's forward multiple, by date.
 *
 * ⚠⚠ IT USED TO CARRY THE CLOSE AND THE PER-SHARE EACH POINT WAS DIVIDED FROM, and it does not any
 * more because the line those belonged to was removed (2026-08-21, on request — see
 * `MultipleHistoryChart`). That is not lost detail: the trailing multiple was OUR division and so
 * had two operands worth showing, while the forward one is GuruFocus's published indicator, read
 * straight through. A "Close ÷ EPS" pair beside it would be arithmetic nobody performed.
 *
 * ⚠ IT IS HANDED THE PLOTTED ROWS, NOT AN ISIN — the same rule as `QuickValuationInputsModal`, so
 * the table cannot arrive at a different multiple than the line it was opened from.
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
  rows, basis: b, median, currency, name, isin, fromYear, onClose,
}: {
  /** The rows the chart plots — `t`, `fwd`. */
  rows: AlignedRow[];
  basis: (typeof BASIS)[keyof typeof BASIS];
  median: number | null;
  currency?: string | null;
  name?: string | null;
  isin: string;
  fromYear: number;
  onClose: () => void;
}) {
  const ceiling = median == null ? null : median * OUTLIER_MULT;

  const ordered = [...rows].sort((a, b2) => b2.t - a.t);
  const mult = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(1)}×`);
  const day = (t: number) => new Date(t).toISOString().slice(0, 10);

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[88vw] h-[84vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">{b.multiple} — forward, by date</h2>
          {name && <span className="text-sm text-fg-soft truncate max-w-[28ch]" title={name}>{name}</span>}
          <span className="text-[12px] font-mono text-fg-faint">{isin}</span>
          <button type="button" onClick={onClose}
            className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-3">
          <p className="text-[12px] text-fg-faint">
            Forward {b.multiple} is GuruFocus&apos;s own published indicator, read straight through
            and computed from nothing here — which is why there is no close and no {b.perShare}
            {' '}beside it to divide.
            {' '}Weekly since {fromYear}, newest first · {ordered.length} rows · Source: {b.source}
          </p>

          <div className="overflow-auto rounded-lg border border-neutral-800/40">
            <table className="w-full text-xs">
              <thead className="bg-page sticky top-0 z-10">
                <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
                  <th className="px-3 py-1.5 font-medium text-left">Date</th>
                  <th className="px-3 py-1.5 font-medium text-right">
                    Forward {b.multiple}{currency ? ` (${currency})` : ''}
                  </th>
                </tr>
              </thead>
              <tbody>
                {ordered.map((r) => {
                  // The rows the chart footer counts as "off the top of the axis". They are the
                  // reason to open this table at all, so they are findable in it.
                  const over = ceiling != null && r.fwd != null && r.fwd > ceiling;
                  return (
                    <tr key={r.t} className="border-t border-neutral-800/40 hover:bg-overlay/[0.02]">
                      <td className="px-3 py-1 whitespace-nowrap font-mono text-fg-soft">{day(r.t)}</td>
                      {/* ⚠ TEXT WEARS TEXT TOKENS, NEVER THE SERIES COLOUR — the house dataviz
                          rule. The amber here is the warning ramp, not the chart's amber line. */}
                      <td className={`px-3 py-1 text-right font-mono font-medium ${over ? 'text-warn-300' : 'text-fg-soft'}`}
                        title={over ? `Above ${OUTLIER_MULT}× the median (${mult(median)}) — a collapsed denominator, not a valuation. Drawn, but off the top of the chart's axis.` : undefined}>
                        {over ? '⚠ ' : ''}{mult(r.fwd)}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {/* ⚠ THE "blank Close is not a missing price" NOTE WENT WITH THE TRAILING COLUMNS. It
              explained rows whose timestamp came from the OTHER series; with one series every row
              is an observation of it, so there is nothing left to explain. */}
        </div>
      </div>
    </div>
  );
}
