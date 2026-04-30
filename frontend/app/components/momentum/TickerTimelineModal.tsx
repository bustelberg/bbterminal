'use client';

import { Fragment, useEffect, useMemo, useState } from 'react';
import type { BacktestResult } from '../../../lib/stores/momentum';
import CellInfoTip from './CellInfoTip';
import type { ScoringConfig } from './MonthlyHoldingsTable';
import { annualize, fmtPct, guruFocusUrl, EXCHANGE_NAMES } from './utils';
import { runSSE } from '../../../lib/stream';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type Props = {
  result: BacktestResult;
  companyId: number | null;
  exchangeByCompany: Map<number, string>;
  scoringConfig: ScoringConfig;
  onClose: () => void;
};

type SignalComponent = { label: string; value_str?: string };
type SignalBreakdown = {
  key: string;
  label: string;
  description: string;
  category: string;
  raw_value: number | null;
  components: SignalComponent[];
  universe_min: number | null;
  universe_max: number | null;
  normalized_score: number | null;
  weight: number;
};
type CategoryScore = {
  category: string;
  score: number | null;
  weight: number;
  contribution: number | null;
};
type BreakdownData = {
  company_id: number;
  ticker: string;
  exchange: string;
  company_name: string;
  as_of_date: string;
  anchor_date: string;
  anchor_price: number;
  signals: SignalBreakdown[];
  category_scores: CategoryScore[];
  category_weights_normalized: Record<string, number>;
  momentum_score: number | null;
  universe_size: number;
  in_universe_at_cutoff: boolean;
  universe_label_used: string | null;
};
type BreakdownState =
  | { status: 'loading'; pct: number; message: string }
  | { status: 'error'; message: string }
  | { status: 'ok'; data: BreakdownData };

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
export default function TickerTimelineModal({ result, companyId, exchangeByCompany, scoringConfig, onClose }: Props) {
  // Per-visit breakdown state. Keyed by the visit's start month so cached
  // breakdowns survive while the user toggles other rows.
  const [expandedVisitIdx, setExpandedVisitIdx] = useState<number | null>(null);
  const [breakdowns, setBreakdowns] = useState<Record<number, BreakdownState>>({});

  // Reset cached breakdowns when the user opens a different company.
  useEffect(() => {
    setExpandedVisitIdx(null);
    setBreakdowns({});
  }, [companyId]);

  const fetchBreakdown = async (visitIdx: number, asOfDate: string, cid: number) => {
    setBreakdowns((prev) => ({
      ...prev,
      [visitIdx]: { status: 'loading', pct: 0, message: 'Starting...' },
    }));
    let receivedResult = false;
    try {
      await runSSE(
        `${API_URL}/api/momentum/signal-breakdown`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            company_id: cid,
            as_of_date: asOfDate,
            universe_label: scoringConfig.universe_label,
            index_universe: scoringConfig.index_universe,
            signal_weights: scoringConfig.signal_weights,
            category_weights: scoringConfig.category_weights,
          }),
        },
        (raw) => {
          const evt = raw as { type?: string; pct?: number; message?: string; data?: BreakdownData };
          if (evt.type === 'progress') {
            setBreakdowns((prev) => ({
              ...prev,
              [visitIdx]: {
                status: 'loading',
                pct: typeof evt.pct === 'number' ? evt.pct : 0,
                message: evt.message ?? '',
              },
            }));
          } else if (evt.type === 'result' && evt.data) {
            receivedResult = true;
            setBreakdowns((prev) => ({ ...prev, [visitIdx]: { status: 'ok', data: evt.data as BreakdownData } }));
          } else if (evt.type === 'error') {
            receivedResult = true;
            setBreakdowns((prev) => ({
              ...prev,
              [visitIdx]: { status: 'error', message: evt.message ?? 'Unknown error' },
            }));
          }
        },
      );
      if (!receivedResult) {
        setBreakdowns((prev) => ({
          ...prev,
          [visitIdx]: { status: 'error', message: 'Stream ended without a result' },
        }));
      }
    } catch (e) {
      setBreakdowns((prev) => ({
        ...prev,
        [visitIdx]: { status: 'error', message: e instanceof Error ? e.message : String(e) },
      }));
    }
  };

  const toggleVisit = (visitIdx: number, asOfDate: string) => {
    if (expandedVisitIdx === visitIdx) {
      setExpandedVisitIdx(null);
      return;
    }
    setExpandedVisitIdx(visitIdx);
    if (companyId != null && !breakdowns[visitIdx]) {
      fetchBreakdown(visitIdx, asOfDate, companyId);
    }
  };
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
                  const isOpen = expandedVisitIdx === i;
                  const breakdown = breakdowns[i];
                  // The visit's selection happened at the start month — so
                  // the breakdown asks the backend "what did the strategy
                  // see at YYYY-MM-01 that led to picking this stock?"
                  const asOfDate = `${start.date}-01`;
                  return (
                    <Fragment key={i}>
                    <tr
                      className="border-b border-gray-800/20 cursor-pointer hover:bg-white/[0.02]"
                      onClick={() => toggleVisit(i, asOfDate)}
                    >
                      <td className="py-1.5 text-gray-400 font-mono">
                        <span className="text-gray-600 mr-1">{isOpen ? '▾' : '▸'}</span>
                        {i + 1}
                      </td>
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
                    {isOpen && (
                      <tr key={`${i}-breakdown`}>
                        <td colSpan={6} className="bg-[#0f1117] px-4 py-3 border-b border-gray-800/30">
                          {!breakdown && <div className="text-xs text-gray-500">Click again to load breakdown.</div>}
                          {breakdown?.status === 'loading' && (
                            <div className="space-y-1.5">
                              <div className="flex items-center justify-between text-[11px] text-gray-400">
                                <span className="flex items-center gap-2">
                                  <svg className="animate-spin w-3 h-3 text-indigo-400" viewBox="0 0 24 24" fill="none">
                                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                                  </svg>
                                  Computing breakdown for {start.date}
                                </span>
                                <span className="font-mono">{breakdown.pct}%</span>
                              </div>
                              <div className="h-1.5 bg-gray-800 rounded-full overflow-hidden">
                                <div
                                  className="h-full bg-indigo-500 transition-all duration-200"
                                  style={{ width: `${breakdown.pct}%` }}
                                />
                              </div>
                              {breakdown.message && (
                                <div className="text-[11px] text-gray-500 font-mono">{breakdown.message}</div>
                              )}
                            </div>
                          )}
                          {breakdown?.status === 'error' && (
                            <div className="text-xs text-rose-400">
                              Could not load breakdown: {breakdown.message}
                            </div>
                          )}
                          {breakdown?.status === 'ok' && (
                            <BreakdownView data={breakdown.data} />
                          )}
                        </td>
                      </tr>
                    )}
                    </Fragment>
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

/** Renders the per-visit breakdown returned by /api/momentum/signal-breakdown.
 * Each signal shows its raw value, the basis numbers it came from, the
 * universe-wide min/max that drove the 0-100 normalization, and the weight.
 * The category roll-up at the bottom mirrors the scoring engine's math. */
function BreakdownView({ data }: { data: BreakdownData }) {
  const fmtRaw = (v: number | null) => (v == null ? '—' : Number.isInteger(v) ? `${v}` : `${v.toFixed(4)}`);
  return (
    <div className="space-y-3">
      <div className="text-[11px] text-gray-500">
        Anchor: latest close strictly before {data.as_of_date} ={' '}
        <span className="text-gray-300 font-mono">{data.anchor_price.toFixed(4)}</span> on{' '}
        <span className="text-gray-300 font-mono">{data.anchor_date}</span>{' '}
        · universe size at this cutoff:{' '}
        <span className="text-gray-300 font-mono">{data.universe_size}</span>
        {data.universe_label_used && (
          <span> · scoped to <span className="text-gray-300 font-mono">{data.universe_label_used}</span></span>
        )}
        {!data.in_universe_at_cutoff && (
          <span className="text-amber-400">
            {' '}· note: this company was not in the universe at this cutoff, so the displayed normalized scores may differ from the live selection
          </span>
        )}
      </div>

      {/* Per-signal breakdown */}
      <div className="space-y-2">
        {data.signals.map((s) => {
          const rangeStr = (s.universe_min != null && s.universe_max != null)
            ? `[${fmtRaw(s.universe_min)}, ${fmtRaw(s.universe_max)}]`
            : '—';
          return (
            <div key={s.key} className="rounded-md border border-gray-800/60 px-3 py-2">
              <div className="flex items-baseline justify-between gap-3 flex-wrap">
                <div className="flex items-baseline gap-2">
                  <span className="text-gray-200 text-xs font-medium">{s.label}</span>
                  <span className="text-[10px] text-gray-600">·</span>
                  <span className="text-[10px] text-gray-500 capitalize">{s.category}</span>
                  <span className="text-[10px] text-gray-600">· weight</span>
                  <span className="text-[10px] text-gray-300 font-mono">{s.weight}</span>
                </div>
                <div className="text-[11px] flex items-center gap-3 font-mono">
                  <span>
                    <span className="text-gray-500">raw </span>
                    <span className="text-gray-100">{fmtRaw(s.raw_value)}</span>
                  </span>
                  <span>
                    <span className="text-gray-500">norm </span>
                    <span className="text-indigo-300">{s.normalized_score != null ? `${s.normalized_score.toFixed(1)}` : '—'}</span>
                    <span className="text-gray-600"> /100</span>
                  </span>
                </div>
              </div>
              <div className="text-[11px] text-gray-500 mt-0.5">
                Universe range: <span className="font-mono">{rangeStr}</span>
              </div>
              <ul className="mt-1.5 space-y-0.5">
                {s.components.map((c, idx) => (
                  <li key={idx} className="text-[11px] text-gray-400 flex gap-2">
                    <span className="text-gray-600 shrink-0">↳</span>
                    <span className="shrink-0">{c.label}</span>
                    {c.value_str && <span className="text-gray-200 font-mono ml-auto">{c.value_str}</span>}
                  </li>
                ))}
              </ul>
            </div>
          );
        })}
      </div>

      {/* Per-category math: how each signal's normalized score rolls up into
          the category score. Backend's scoring engine does
            score_cat = Σ (signal_norm × signal_weight / Σ weights_in_cat)
          so the per-signal "share" within a category is just its weight as a
          fraction of the category's total weight. */}
      <CategoryMathBreakdown data={data} />

      {/* Final roll-up: per-category scores combined via the category weights. */}
      <div className="rounded-md border border-indigo-500/20 bg-indigo-500/[0.04] px-3 py-2">
        <div className="text-[11px] text-gray-400 mb-1.5">
          Final score · combine per-category scores via the category weights:
          <span className="font-mono"> Σ (category_score × category_weight) = momentum_score</span>.
        </div>
        <table className="w-full text-[11px]">
          <thead>
            <tr className="text-gray-600">
              <th className="text-left font-medium">Category</th>
              <th className="text-right font-medium">Score (0–100)</th>
              <th className="text-right font-medium">Weight</th>
              <th className="text-right font-medium">Contribution = score × weight</th>
            </tr>
          </thead>
          <tbody>
            {data.category_scores.map((c) => (
              <tr key={c.category} className="border-t border-gray-800/30">
                <td className="py-1 capitalize text-gray-300">{c.category}</td>
                <td className="py-1 text-right font-mono text-gray-100">{c.score != null ? c.score.toFixed(2) : '—'}</td>
                <td className="py-1 text-right font-mono text-gray-300">{(c.weight * 100).toFixed(0)}%</td>
                <td className="py-1 text-right font-mono text-gray-100">{c.contribution != null ? c.contribution.toFixed(2) : '—'}</td>
              </tr>
            ))}
            <tr className="border-t border-indigo-500/30">
              <td className="py-1.5 text-gray-200 font-medium">Final momentum_score</td>
              <td colSpan={2} />
              <td className="py-1.5 text-right font-mono text-indigo-300 font-medium">
                {data.momentum_score != null ? data.momentum_score.toFixed(2) : '—'}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

/** Shows per-category arithmetic: for each category, the active signals,
 * their weight share within the category, normalized 0-100 score, and
 * each signal's contribution to the category score. The sum of
 * contributions equals the category score the scoring engine produced. */
function CategoryMathBreakdown({ data }: { data: BreakdownData }) {
  // Group signals by category; only count signals whose weight > 0 — those
  // are the "active" ones the scoring engine includes (zero-weight signals
  // are filtered out in `_score_category`).
  const byCategory = new Map<string, SignalBreakdown[]>();
  for (const s of data.signals) {
    if (s.weight <= 0) continue;
    const arr = byCategory.get(s.category) ?? [];
    arr.push(s);
    byCategory.set(s.category, arr);
  }

  if (byCategory.size === 0) return null;

  return (
    <div className="space-y-2">
      <div className="text-[11px] text-gray-400">
        Category math · each signal&apos;s weight share within its category × its 0–100 normalized score = contribution; the sum is the category score the scoring engine produced.
      </div>
      {Array.from(byCategory.entries()).map(([cat, sigs]) => {
        const totalWeight = sigs.reduce((s, x) => s + x.weight, 0);
        // The category score from the scoring engine, for the cross-check footer.
        const cs = data.category_scores.find((c) => c.category === cat);
        const checkSum = sigs.reduce((acc, s) => {
          if (s.normalized_score == null || totalWeight <= 0) return acc;
          return acc + (s.normalized_score * s.weight) / totalWeight;
        }, 0);
        return (
          <div key={cat} className="rounded-md border border-gray-800/60 px-3 py-2">
            <div className="flex items-baseline justify-between mb-1.5">
              <div>
                <span className="text-gray-200 text-xs font-medium capitalize">{cat}</span>
                <span className="text-[10px] text-gray-500 ml-2">category</span>
              </div>
              <div className="text-[11px]">
                <span className="text-gray-500">score </span>
                <span className="text-indigo-300 font-mono">
                  {cs?.score != null ? cs.score.toFixed(2) : '—'}
                </span>
                <span className="text-gray-600"> /100</span>
              </div>
            </div>
            <table className="w-full text-[11px]">
              <thead>
                <tr className="text-gray-600">
                  <th className="text-left font-medium">Signal</th>
                  <th className="text-right font-medium">Normalized</th>
                  <th className="text-right font-medium">Weight</th>
                  <th className="text-right font-medium">Share</th>
                  <th className="text-right font-medium">Contribution</th>
                </tr>
              </thead>
              <tbody>
                {sigs.map((s) => {
                  const share = totalWeight > 0 ? s.weight / totalWeight : 0;
                  const contribution = s.normalized_score != null ? s.normalized_score * share : null;
                  return (
                    <tr key={s.key} className="border-t border-gray-800/30">
                      <td className="py-1 text-gray-300">{s.label}</td>
                      <td className="py-1 text-right font-mono text-gray-100">
                        {s.normalized_score != null ? s.normalized_score.toFixed(2) : '—'}
                      </td>
                      <td className="py-1 text-right font-mono text-gray-300">{s.weight}</td>
                      <td className="py-1 text-right font-mono text-gray-300">{(share * 100).toFixed(1)}%</td>
                      <td className="py-1 text-right font-mono text-gray-100">
                        {contribution != null ? contribution.toFixed(2) : '—'}
                      </td>
                    </tr>
                  );
                })}
                <tr className="border-t border-gray-700/50">
                  <td className="py-1 text-gray-500 italic">Σ (share × normalized)</td>
                  <td colSpan={3} />
                  <td className="py-1 text-right font-mono text-indigo-300">
                    {Number.isFinite(checkSum) ? checkSum.toFixed(2) : '—'}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        );
      })}
    </div>
  );
}
