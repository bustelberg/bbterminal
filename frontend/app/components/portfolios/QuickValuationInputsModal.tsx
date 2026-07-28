'use client';

import { fcfYieldOf, type Rebased, type YearPoint } from './quickValuation';

/**
 * The data behind the Price-vs-FCF/share chart: the two raw lines as reported, then the two indexed
 * lines the chart actually draws. Same shape as the Long Equity drill-downs — raw inputs first,
 * derived rows under them.
 *
 * ⚠ IT IS HANDED THE COMPUTED SERIES, NOT AN ISIN. The tab already built these points and this
 * index; refetching and re-deriving them here would be a second computation that can disagree with
 * the line it claims to explain — the failure every other drill-down in this folder is written to
 * avoid.
 */
export default function QuickValuationInputsModal({
  points, index, currency, name, isin, onClose,
}: {
  points: YearPoint[];
  index: Rebased;
  currency?: string | null;
  name?: string | null;
  isin: string;
  onClose: () => void;
}) {
  const years = points.map((p) => p.year);
  const byYear = new Map(index.rows.map((r) => [r.year, r]));
  const num = (v: number | null | undefined, d = 2) => (v == null ? '—' : v.toFixed(d));

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[88vw] h-[84vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">Price vs FCF / share — by fiscal year</h2>
          {name && <span className="text-sm text-fg-soft truncate max-w-[28ch]" title={name}>{name}</span>}
          <span className="text-[11px] font-mono text-fg-faint">{isin}</span>
          <button type="button" onClick={onClose} className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-3">
          <p className="text-[11px] text-fg-faint">
            Price is the close at each FISCAL year end and free cash flow per share is as reported —
            both {currency ? `in ${currency}, ` : ''}from the same fiscal rows. The indexed rows are
            what the chart draws{index.anchor != null ? `, each 100 at FY${index.anchor}` : ''}.
          </p>

          <div className="overflow-auto rounded-lg border border-neutral-800/40">
            <table className="w-full text-xs">
              <thead className="bg-page">
                <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                  <th className="px-3 py-1.5 font-medium text-left sticky left-0 bg-page z-10">Line</th>
                  {years.map((y) => <th key={y} className="px-3 py-1.5 font-medium text-right">{y}</th>)}
                </tr>
              </thead>
              <tbody>
                <tr className="border-t border-neutral-800/40 hover:bg-overlay/[0.02]">
                  <td className="px-3 py-1 whitespace-nowrap text-fg-soft sticky left-0 bg-card z-10">
                    Year-end price{currency ? ` (${currency})` : ''}
                  </td>
                  {points.map((p) => (
                    <td key={p.year} className="px-3 py-1 text-right font-mono text-fg-soft">{num(p.price)}</td>
                  ))}
                </tr>
                <tr className="hover:bg-overlay/[0.02]">
                  <td className="px-3 py-1 whitespace-nowrap text-fg-soft sticky left-0 bg-card z-10">
                    FCF / share{currency ? ` (${currency})` : ''}
                  </td>
                  {points.map((p) => (
                    // A cash-burn year is a real observation, marked so it doesn't read as a small
                    // positive at a glance — it is the year the index rebase had to work around.
                    <td key={p.year}
                      className={`px-3 py-1 text-right font-mono ${p.fcf != null && p.fcf < 0 ? 'text-neg-300' : 'text-fg-soft'}`}>
                      {num(p.fcf)}
                    </td>
                  ))}
                </tr>
                {/* The two derived rows — what the chart plots. */}
                <tr className="border-t border-neutral-800/40 hover:bg-overlay/[0.02]">
                  <td className="px-3 py-1 whitespace-nowrap text-fg-soft font-medium sticky left-0 bg-card z-10">
                    Price (index)
                  </td>
                  {years.map((y) => (
                    <td key={y} className="px-3 py-1 text-right font-mono text-fg-soft font-medium">
                      {num(byYear.get(y)?.price, 0)}
                    </td>
                  ))}
                </tr>
                <tr className="hover:bg-overlay/[0.02]">
                  <td className="px-3 py-1 whitespace-nowrap text-fg-soft font-medium sticky left-0 bg-card z-10">
                    FCF / share (index)
                  </td>
                  {years.map((y) => (
                    <td key={y} className="px-3 py-1 text-right font-mono text-fg-soft font-medium">
                      {num(byYear.get(y)?.fcf, 0)}
                    </td>
                  ))}
                </tr>
                {/* The second chart's line, from the same two raw rows above. */}
                <tr className="border-t border-neutral-800/40 hover:bg-overlay/[0.02]">
                  <td className="px-3 py-1 whitespace-nowrap text-fg-soft font-medium sticky left-0 bg-card z-10">
                    FCF yield
                  </td>
                  {points.map((p) => {
                    const y = fcfYieldOf(p.fcf, p.price);
                    return (
                      <td key={p.year}
                        className={`px-3 py-1 text-right font-mono font-medium ${y != null && y < 0 ? 'text-neg-300' : 'text-fg-soft'}`}>
                        {y == null ? '—' : `${y.toFixed(2)}%`}
                      </td>
                    );
                  })}
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
