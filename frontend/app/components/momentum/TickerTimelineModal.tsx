'use client';

import { useEffect, useMemo } from 'react';
import type { BacktestResult } from '../../../lib/stores/momentum';
import CellInfoTip from './CellInfoTip';
import { annualize, fmtPct, guruFocusUrl, EXCHANGE_NAMES } from './utils';

type Props = {
  result: BacktestResult;
  companyId: number | null;
  exchangeByCompany: Map<number, string>;
  onClose: () => void;
};

type MonthCell = {
  date: string;            // YYYY-MM
  held: boolean;
  forwardReturnPct: number | null;
  score: number | null;
  sector: string | null;
};

/** Modal showing one company's holding history across the entire backtest:
 * a horizontal heatmap strip where each cell = one month, colored when held
 * (green/red gradient by that month's forward return). Hover for details.
 */
export default function TickerTimelineModal({ result, companyId, exchangeByCompany, onClose }: Props) {
  // Pre-compute the per-month cell data for the selected company. Also
  // grab a representative ticker/name/sector from any month it was held.
  const { cells, ticker, companyName, sector, summary } = useMemo(() => {
    const cells: MonthCell[] = [];
    let ticker = '';
    let companyName = '';
    let lastSector: string | null = null;
    let monthsHeld = 0;
    let totalRet = 1.0;
    let bestRet = -Infinity;
    let bestRetMonth = '';
    let worstRet = Infinity;
    let worstRetMonth = '';

    for (const r of result.monthly_records) {
      const h = companyId == null
        ? null
        : r.holdings.find((x) => x.company_id === companyId);
      const cell: MonthCell = {
        date: r.date,
        held: !!h,
        forwardReturnPct: h?.forward_return_pct ?? null,
        score: h ? h.score : null,
        sector: h ? h.sector : null,
      };
      cells.push(cell);

      if (h) {
        monthsHeld += 1;
        if (!ticker) ticker = h.ticker;
        if (!companyName) companyName = h.company_name;
        lastSector = h.sector;
        if (h.forward_return_pct != null) {
          totalRet *= 1 + h.forward_return_pct / 100;
          if (h.forward_return_pct > bestRet) {
            bestRet = h.forward_return_pct;
            bestRetMonth = r.date;
          }
          if (h.forward_return_pct < worstRet) {
            worstRet = h.forward_return_pct;
            worstRetMonth = r.date;
          }
        }
      }
    }

    const compoundReturnPct = monthsHeld > 0 ? (totalRet - 1) * 100 : null;
    const summary = {
      monthsHeld,
      totalMonths: result.monthly_records.length,
      pct: result.monthly_records.length > 0
        ? (monthsHeld / result.monthly_records.length) * 100
        : 0,
      compoundReturnPct,
      cagrPct: annualize(compoundReturnPct, monthsHeld),
      bestRet: bestRet === -Infinity ? null : bestRet,
      bestRetMonth: bestRetMonth || null,
      worstRet: worstRet === Infinity ? null : worstRet,
      worstRetMonth: worstRetMonth || null,
    };

    return { cells, ticker, companyName, sector: lastSector, summary };
  }, [result, companyId]);

  // Close on ESC.
  useEffect(() => {
    if (companyId == null) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [companyId, onClose]);

  if (companyId == null) return null;

  const exchange = exchangeByCompany.get(companyId) ?? '';
  const href = ticker ? guruFocusUrl(ticker, exchange) : null;

  // Identify run boundaries (a "buy" = transition from not-held → held;
  // "sell" = transition from held → not-held). Grouping consecutive
  // held months helps the user count distinct visits.
  type Run = { startIdx: number; endIdx: number };
  const runs: Run[] = [];
  let curStart: number | null = null;
  for (let i = 0; i < cells.length; i++) {
    if (cells[i].held && curStart === null) curStart = i;
    else if (!cells[i].held && curStart !== null) {
      runs.push({ startIdx: curStart, endIdx: i - 1 });
      curStart = null;
    }
  }
  if (curStart !== null) runs.push({ startIdx: curStart, endIdx: cells.length - 1 });

  // Color mapping for the cells: green/red intensity by forward return, gray
  // bar above the cell for "not held" (rendered as low-opacity backdrop).
  const cellColor = (c: MonthCell): string => {
    if (!c.held) return 'rgba(75, 85, 99, 0.15)';
    const r = c.forwardReturnPct;
    if (r == null) return 'rgba(99, 102, 241, 0.45)'; // indigo when held but no return
    if (r >= 0) {
      // emerald, intensity by magnitude (cap at 20%)
      const a = 0.30 + Math.min(1, r / 20) * 0.55;
      return `rgba(16, 185, 129, ${a})`;
    }
    const a = 0.30 + Math.min(1, -r / 20) * 0.55;
    return `rgba(244, 63, 94, ${a})`;
  };

  return (
    <div
      className="fixed inset-0 z-[200] flex items-center justify-center bg-black/60 backdrop-blur-sm"
      onClick={onClose}
    >
      <div
        className="bg-[#151821] border border-gray-800/60 rounded-xl shadow-2xl w-[92%] max-w-5xl max-h-[90vh] overflow-auto"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-800/40 flex items-start justify-between gap-4">
          <div>
            <div className="flex items-center gap-3">
              <h2 className="text-white text-base font-semibold font-mono">{ticker || '—'}</h2>
              {exchange && (
                <span
                  className="text-[10px] text-gray-500"
                  title={EXCHANGE_NAMES[exchange.toUpperCase()] ?? exchange}
                >
                  {exchange}
                </span>
              )}
              {sector && <span className="text-xs text-gray-500">{sector}</span>}
              {href && (
                <a
                  href={href}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-xs text-indigo-400 hover:text-indigo-300 hover:underline"
                >
                  GuruFocus ↗
                </a>
              )}
            </div>
            <div className="text-sm text-gray-300 mt-1">{companyName}</div>
          </div>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-white transition-colors p-1"
            aria-label="Close"
          >
            <svg className="w-5 h-5" viewBox="0 0 20 20" fill="currentColor">
              <path
                fillRule="evenodd"
                d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z"
                clipRule="evenodd"
              />
            </svg>
          </button>
        </div>

        {/* Summary stats */}
        <div className="px-6 py-3 border-b border-gray-800/40 grid grid-cols-2 md:grid-cols-5 gap-3 text-xs">
          <div className="bg-[#0f1117] rounded-lg px-3 py-2">
            <div className="text-gray-500 flex items-center gap-1">
              Months held
              <CellInfoTip>
                Number of months the strategy held this stock during the backtest, out of the total months in the backtest. The percentage shows what fraction of the backtest this stock was active.
              </CellInfoTip>
            </div>
            <div className="font-mono text-gray-200">
              {summary.monthsHeld} / {summary.totalMonths}
              <span className="text-gray-500"> ({summary.pct.toFixed(1)}%)</span>
            </div>
          </div>
          <div className="bg-[#0f1117] rounded-lg px-3 py-2">
            <div className="text-gray-500 flex items-center gap-1">
              Distinct visits
              <CellInfoTip>
                Number of separate buy → sell cycles. A "visit" is a continuous run of months where the stock was held; if the strategy dropped it for a month and re-bought later, that&apos;s two visits.
              </CellInfoTip>
            </div>
            <div className="font-mono text-gray-200">{runs.length}</div>
          </div>
          <div className="bg-[#0f1117] rounded-lg px-3 py-2">
            <div className="text-gray-500 flex items-center gap-1">
              Compound return (held)
              <CellInfoTip>
                Cumulative return across all months the stock was held: chain-link the monthly forward returns —
                <span className="font-mono"> (1+r₁)(1+r₂)…(1+rₙ) − 1</span>.
                Months when not held don&apos;t contribute, so this answers "what was the strategy&apos;s realized return on this stock during its visits?".
              </CellInfoTip>
            </div>
            <div className={`font-mono ${summary.compoundReturnPct != null && summary.compoundReturnPct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
              {fmtPct(summary.compoundReturnPct)}
            </div>
            <div className={`text-[10px] font-mono mt-0.5 flex items-center gap-1 ${summary.cagrPct != null && summary.cagrPct >= 0 ? 'text-emerald-500/80' : 'text-rose-500/80'}`}>
              <span className="text-gray-500">CAGR</span>
              <span>{fmtPct(summary.cagrPct)}</span>
              <CellInfoTip>
                Annualized version of the compound return:
                <span className="font-mono"> (1 + compound_return)^(12 / months_held) − 1</span>.
                Useful for comparing stocks held for different durations on the same scale. Noisy when total months held is small (less than ~12 the math extrapolates a short window to a full year).
              </CellInfoTip>
            </div>
          </div>
          <div className="bg-[#0f1117] rounded-lg px-3 py-2">
            <div className="text-gray-500 flex items-center gap-1">
              Best month
              <CellInfoTip>
                The single month with the highest forward return while held, plus when it occurred. Forward return = (next-month entry price ÷ this-month entry price) − 1, in EUR.
              </CellInfoTip>
            </div>
            <div className="font-mono text-emerald-400">
              {fmtPct(summary.bestRet)}
              {summary.bestRetMonth && (
                <span className="text-gray-500 text-[10px] ml-1">{summary.bestRetMonth}</span>
              )}
            </div>
          </div>
          <div className="bg-[#0f1117] rounded-lg px-3 py-2">
            <div className="text-gray-500 flex items-center gap-1">
              Worst month
              <CellInfoTip>
                The single month with the lowest forward return while held, plus when it occurred. Same definition as Best month, just the floor.
              </CellInfoTip>
            </div>
            <div className="font-mono text-rose-400">
              {fmtPct(summary.worstRet)}
              {summary.worstRetMonth && (
                <span className="text-gray-500 text-[10px] ml-1">{summary.worstRetMonth}</span>
              )}
            </div>
          </div>
        </div>

        {/* Timeline strip — one cell per month across the whole backtest. */}
        <div className="px-6 py-4">
          <div className="text-xs text-gray-400 mb-2">Holding timeline (each cell is one month — green/red intensity = that month&apos;s forward return; gray = not held)</div>
          <div className="flex gap-[2px] overflow-x-auto py-1">
            {cells.map((c) => (
              <div
                key={c.date}
                className="relative group shrink-0"
                style={{ width: 8, height: 28 }}
              >
                <div
                  className="absolute inset-0 rounded-sm"
                  style={{ background: cellColor(c) }}
                />
                {/* Hover tooltip */}
                <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 hidden group-hover:block z-10 pointer-events-none">
                  <div className="bg-[#1e2130] border border-gray-700 rounded-md px-2 py-1.5 text-[11px] whitespace-nowrap shadow-xl">
                    <div className="font-mono text-gray-300">{c.date}</div>
                    {c.held ? (
                      <>
                        <div className="text-gray-400">
                          Return: <span className={c.forwardReturnPct != null && c.forwardReturnPct >= 0 ? 'text-emerald-400' : 'text-rose-400'}>
                            {fmtPct(c.forwardReturnPct)}
                          </span>
                        </div>
                        {c.score != null && (
                          <div className="text-gray-400">Score: <span className="font-mono text-gray-200">{c.score.toFixed(1)}</span></div>
                        )}
                        {c.sector && <div className="text-gray-500">{c.sector}</div>}
                      </>
                    ) : (
                      <div className="text-gray-500">Not held</div>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>

          {/* Year-boundary axis labels under the strip. */}
          <div className="flex gap-[2px] mt-1">
            {cells.map((c, i) => {
              const month = c.date.slice(5, 7);
              const showYear = month === '01' || i === 0;
              const year = c.date.slice(0, 4);
              return (
                <div
                  key={`ax-${c.date}`}
                  className="text-[9px] text-gray-600 font-mono shrink-0 text-left"
                  style={{ width: 8 }}
                >
                  {showYear ? year : ''}
                </div>
              );
            })}
          </div>
        </div>

        {/* Per-visit table */}
        {runs.length > 0 && (
          <div className="px-6 pb-5">
            <div className="text-xs text-gray-400 mb-2">Visits</div>
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-500 border-b border-gray-800/40">
                  <th className="text-left py-1.5 font-medium">
                    <span className="inline-flex items-center gap-1">
                      #
                      <CellInfoTip>Visit number, in chronological order. 1 = first time the strategy held this stock.</CellInfoTip>
                    </span>
                  </th>
                  <th className="text-left py-1.5 font-medium">
                    <span className="inline-flex items-center gap-1">
                      Bought
                      <CellInfoTip>First month of the visit — the rebalance month at which the strategy started holding this stock for this run.</CellInfoTip>
                    </span>
                  </th>
                  <th className="text-left py-1.5 font-medium">
                    <span className="inline-flex items-center gap-1">
                      Sold
                      <CellInfoTip>First month after the visit ended — i.e. the rebalance at which the strategy stopped holding this stock. Shows "— still held" if the visit reaches the end of the backtest.</CellInfoTip>
                    </span>
                  </th>
                  <th className="text-right py-1.5 font-medium">
                    <span className="inline-flex items-center gap-1">
                      Months
                      <CellInfoTip>Duration of this visit in months — the number of consecutive rebalance periods the stock was held.</CellInfoTip>
                    </span>
                  </th>
                  <th className="text-right py-1.5 font-medium">
                    <span className="inline-flex items-center gap-1">
                      Compound return
                      <CellInfoTip>
                        Chain-linked return across the months in this visit:
                        <span className="font-mono"> (1+r₁)(1+r₂)…(1+rₙ) − 1</span>,
                        where each <span className="font-mono">rᵢ</span> is the forward return for that month.
                      </CellInfoTip>
                    </span>
                  </th>
                  <th className="text-right py-1.5 font-medium">
                    <span className="inline-flex items-center gap-1">
                      CAGR
                      <CellInfoTip>
                        Annualized return for this visit:
                        <span className="font-mono"> (1 + compound_return)^(12 / months) − 1</span>.
                        Lets you compare a 2-month visit to a 36-month one on the same scale; treat sub-12-month CAGRs with caution since the formula extrapolates a short window to a full year.
                      </CellInfoTip>
                    </span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {runs.map((run, i) => {
                  const start = cells[run.startIdx];
                  // The "sell" date is the month AFTER the last held month
                  // (since each month's forward return is held to next-month).
                  // If the run ends at the very last record, treat as "still held".
                  const stillHeld = run.endIdx === cells.length - 1;
                  const sellCell = stillHeld ? null : cells[run.endIdx + 1];
                  let cumulative = 1.0;
                  let usable = 0;
                  for (let j = run.startIdx; j <= run.endIdx; j++) {
                    const r = cells[j].forwardReturnPct;
                    if (r != null) {
                      cumulative *= 1 + r / 100;
                      usable += 1;
                    }
                  }
                  const ret = usable > 0 ? (cumulative - 1) * 100 : null;
                  const monthsInVisit = run.endIdx - run.startIdx + 1;
                  const cagr = annualize(ret, monthsInVisit);
                  return (
                    <tr key={i} className="border-b border-gray-800/20">
                      <td className="py-1.5 text-gray-400 font-mono">{i + 1}</td>
                      <td className="py-1.5 text-gray-200 font-mono">{start.date}</td>
                      <td className="py-1.5 text-gray-200 font-mono">
                        {stillHeld ? (
                          <span className="text-indigo-400">— still held</span>
                        ) : sellCell ? (
                          sellCell.date
                        ) : '—'}
                      </td>
                      <td className="py-1.5 text-right text-gray-300 font-mono">
                        {monthsInVisit}
                      </td>
                      <td className={`py-1.5 text-right font-mono ${ret != null && ret >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                        {fmtPct(ret)}
                      </td>
                      <td className={`py-1.5 text-right font-mono ${cagr != null && cagr >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                        {fmtPct(cagr)}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
