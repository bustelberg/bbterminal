'use client';

import { useMemo } from 'react';
import {
  dividendYieldWorking, estimateCagrWorking, medianPEWorking,
} from './egmInputs';
import { type MetricRow } from './quickValuation';
import { useDeepValuationCopy } from './deepValuationCopy';

/**
 * The raw data behind the assumption hints — the consensus EPS series the "analysts" CAGR is taken
 * from, the five years the median P/E is a median OF, and the dividend-yield observations one of
 * which was picked.
 *
 * ⚠ IT CALLS THE SAME FUNCTIONS THE HINTS DO (`…Working`, of which the scalar hint is the last
 * field). A drill-down that re-derives the number it explains is a second implementation, and the
 * copy is what drifts — leaving a table that disagrees with the figure it was opened from.
 *
 * The hurdle rate is deliberately absent: it is the reader's required return, with no data behind
 * it, so there is nothing to show.
 */

const MAX_YIELD_ROWS = 8;

export default function EgmAssumptionsModal({ metrics, today, currency, name, isin, onClose }: {
  metrics: MetricRow[];
  today: string;
  currency?: string | null;
  name?: string | null;
  isin: string;
  onClose: () => void;
}) {
  const cagr = useMemo(() => estimateCagrWorking(metrics, today), [metrics, today]);
  const pe = useMemo(() => medianPEWorking(metrics), [metrics]);
  const dy = useMemo(() => dividendYieldWorking(metrics), [metrics]);

  const ccy = currency ? `${currency} ` : '';
  const n2 = (v: number | null) => (v == null ? '—' : v.toFixed(2));
  const n1 = (v: number | null) => (v == null ? '—' : v.toFixed(1));

  const t = useDeepValuationCopy();

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-full max-w-4xl h-[84vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">{t.egmModal.title}</h2>
          {name && <span className="text-sm text-fg-soft truncate max-w-[28ch]" title={name}>{name}</span>}
          <span className="text-[12px] font-mono text-fg-faint">{isin}</span>
          <button type="button" onClick={onClose} className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        {/* ⚠ `min-w-0` + `break-words`: a long unbroken token (a metric code) is what forced the
            horizontal scrollbar — a flex/grid child defaults to min-width:auto and will grow past
            its parent rather than wrap. */}
        <div className="flex-1 overflow-y-auto overflow-x-hidden px-6 py-4 space-y-6 min-w-0">
          {/* ── Growth rate ─────────────────────────────────────────────────────────── */}
          <section className="space-y-2 min-w-0">
            <div className="flex items-baseline gap-2 flex-wrap">
              <h3 className="text-sm font-semibold text-fg-strong">{t.egmModal.growthSection}</h3>
              <span className="font-mono text-sm text-accent-400">
                {cagr.cagr == null ? 'n/a' : `${(cagr.cagr * 100).toFixed(1)}%`}
              </span>
            </div>
            <p className="text-[12px] text-fg-faint break-words whitespace-normal max-w-[80ch]">
              {t.egmModal.growthNote}
            </p>
            {cagr.points.length === 0 ? (
              <p className="text-xs text-warn-300 break-words whitespace-normal max-w-[80ch]">{t.egmModal.noEstimates}</p>
            ) : (
              <>
                <div className="overflow-auto rounded-lg border border-neutral-800/40 max-w-full">
                  <table className="w-full text-xs">
                    <thead className="bg-page">
                      <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
                        <th className="px-3 py-1.5 font-medium text-left">{t.egmModal.colFiscalPeriod}</th>
                        <th className="px-3 py-1.5 font-medium text-right">{t.egmModal.colEpsEstimate}{ccy && ` (${currency})`}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {cagr.points.map((p, i) => {
                        const anchor = i === 0 || i === cagr.points.length - 1;
                        return (
                          <tr key={p.date} className="border-t border-neutral-800/40">
                            <td className={`px-3 py-1 ${anchor ? 'text-fg-soft font-medium' : 'text-fg-muted'}`}>
                              {p.date.slice(0, 7)}
                              {anchor && cagr.cagr != null && (
                                <span className="text-fg-faint font-normal ml-2">
                                  {i === 0 ? t.egmModal.from : t.egmModal.to}
                                </span>
                              )}
                            </td>
                            <td className={`px-3 py-1 text-right font-mono ${anchor ? 'text-fg-soft font-medium' : 'text-fg-muted'}`}>
                              {n2(p.eps)}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
                {cagr.cagr != null && cagr.years != null && (
                  <p className="text-[12px] font-mono text-fg-muted break-words whitespace-normal max-w-[80ch]">
                    ({n2(cagr.points[cagr.points.length - 1].eps)} ÷ {n2(cagr.points[0].eps)})
                    ^(1/{cagr.years}) − 1 = {(cagr.cagr * 100).toFixed(1)}%
                  </p>
                )}
                {cagr.cagr == null && (
                  // Two points are needed, and neither may be a loss — the root of a negative
                  // ratio is not a number.
                  <p className="text-[12px] text-warn-300 break-words whitespace-normal max-w-[80ch]">
                    {t.egmModal.noCagrPrefix}{cagr.points.length < 2
                      ? t.egmModal.noCagrOnePoint
                      : t.egmModal.noCagrNonPositive}
                  </p>
                )}
              </>
            )}
          </section>

          {/* ── Exit P/E ────────────────────────────────────────────────────────────── */}
          <section className="space-y-2 min-w-0">
            <div className="flex items-baseline gap-2 flex-wrap">
              <h3 className="text-sm font-semibold text-fg-strong">{t.egmModal.peSection}</h3>
              <span className="font-mono text-sm text-accent-400">
                {pe.median == null ? 'n/a' : `${pe.median.toFixed(1)}x`}
              </span>
            </div>
            <p className="text-[12px] text-fg-faint break-words whitespace-normal max-w-[80ch]">
              {t.egmModal.peNote}
            </p>
            {pe.rows.length === 0 ? (
              <p className="text-xs text-warn-300 break-words whitespace-normal max-w-[80ch]">{t.egmModal.noPriceHistory}</p>
            ) : (
              <div className="overflow-auto rounded-lg border border-neutral-800/40 max-w-full">
                <table className="w-full text-xs">
                  <thead className="bg-page">
                    <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
                      <th className="px-3 py-1.5 font-medium text-left">{t.egmModal.colFiscalYear}</th>
                      <th className="px-3 py-1.5 font-medium text-right">{t.egmModal.colYearEndPrice}{ccy && ` (${currency})`}</th>
                      <th className="px-3 py-1.5 font-medium text-right">{t.egmModal.colEpsNri}</th>
                      <th className="px-3 py-1.5 font-medium text-right">{t.egmModal.colPE}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[...pe.rows].reverse().map((r) => (
                      <tr key={r.year} className="border-t border-neutral-800/40">
                        <td className="px-3 py-1 text-fg-soft">{r.year}</td>
                        <td className="px-3 py-1 text-right font-mono text-fg-soft">{n2(r.price)}</td>
                        <td className={`px-3 py-1 text-right font-mono ${r.eps != null && r.eps <= 0 ? 'text-neg-300' : 'text-fg-soft'}`}>
                          {n2(r.eps)}
                        </td>
                        <td className="px-3 py-1 text-right font-mono text-fg-soft">
                          {/* ⚠ A loss year is SHOWN and excluded, not hidden: its negative multiple
                              would drag the median down and read as "historically cheap". */}
                          {r.pe == null
                            ? <span className="text-fg-faint" title={t.egmModal.excludedTitle}>{t.egmModal.excluded}</span>
                            : `${n1(r.pe)}x`}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                  <tfoot>
                    <tr className="border-t border-neutral-800/40 bg-page font-semibold text-fg-strong">
                      <td className="px-3 py-1.5" colSpan={3}>
                        {t.egmModal.medianOfUsable(
                          String(pe.rows.filter((r) => r.used).length))}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono">
                        {pe.median == null ? '—' : `${n1(pe.median)}x`}
                      </td>
                    </tr>
                  </tfoot>
                </table>
              </div>
            )}
          </section>

          {/* ── Dividend yield ──────────────────────────────────────────────────────── */}
          <section className="space-y-2 min-w-0">
            <div className="flex items-baseline gap-2 flex-wrap">
              <h3 className="text-sm font-semibold text-fg-strong">{t.egmModal.yieldSection}</h3>
              <span className="font-mono text-sm text-accent-400">
                {dy.chosen == null ? 'n/a' : `${dy.chosen.pct.toFixed(2)}%`}
              </span>
            </div>
            <p className="text-[12px] text-fg-faint break-words whitespace-normal max-w-[80ch]">
              {t.egmModal.yieldNote}
            </p>
            {dy.rows.length === 0 ? (
              <p className="text-xs text-warn-300 break-words whitespace-normal max-w-[80ch]">{t.egmModal.noYieldLine}</p>
            ) : (
              <>
                <div className="overflow-auto rounded-lg border border-neutral-800/40 max-w-full">
                  <table className="w-full text-xs">
                    <thead className="bg-page">
                      <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
                        <th className="px-3 py-1.5 font-medium text-left">{t.egmModal.colPeriodEnd}</th>
                        <th className="px-3 py-1.5 font-medium text-left">{t.egmModal.colCadence}</th>
                        <th className="px-3 py-1.5 font-medium text-right">{t.egmModal.colDividendYield}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {dy.rows.slice(0, MAX_YIELD_ROWS).map((r) => (
                        <tr key={`${r.code}-${r.date}`}
                          className={`border-t border-neutral-800/40 ${r.chosen ? 'bg-overlay/5' : ''}`}>
                          <td className={`px-3 py-1 ${r.chosen ? 'text-fg-strong font-medium' : 'text-fg-soft'}`}>
                            {r.date.slice(0, 10)}{r.chosen && <span className="text-accent-400 ml-2">{t.egmModal.inUse}</span>}
                          </td>
                          <td className="px-3 py-1 text-fg-muted">
                            {r.code.startsWith('quarterly')
                              ? t.egmModal.quarterly : t.egmModal.annual}
                          </td>
                          <td className={`px-3 py-1 text-right font-mono ${r.chosen ? 'text-fg-strong font-medium' : 'text-fg-soft'}`}>
                            {r.pct.toFixed(2)}%
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {dy.rows.length > MAX_YIELD_ROWS && (
                  // ⚠ Said, not silently truncated — a table that stops at eight without saying so
                  // reads as the whole record.
                  <p className="text-[11px] text-fg-faint break-words whitespace-normal max-w-[80ch]">
                    {t.egmModal.showingMostRecent(String(MAX_YIELD_ROWS),
                      String(dy.rows.length))}
                  </p>
                )}
              </>
            )}
          </section>

          {/* No hurdle-rate section: it is a pure choice with no data behind it, so there is
              nothing to show — and a section saying so was just noise. */}
        </div>
      </div>
    </div>
  );
}
