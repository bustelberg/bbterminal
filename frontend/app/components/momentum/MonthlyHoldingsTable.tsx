'use client';

import { Fragment, useEffect, useMemo, useState } from 'react';
import type { BacktestResult } from '../../../lib/stores/momentum';
import CellInfoTip from './CellInfoTip';
import CollapsibleCard from './CollapsibleCard';
import TickerTimelineModal from './TickerTimelineModal';
import { EXCHANGE_NAMES, displayExchange, fmtPct, fmtPrice, guruFocusUrl } from './utils';

/** Subset of the active backtest's selection config that the per-ticker
 * timeline modal forwards to the signal-breakdown endpoint. Lets the
 * recompute see the same universe + weights the user actually ran. */
export type ScoringConfig = {
  universe_label: string | null;
  index_universe: string | null;
  signal_weights: Record<string, number>;
  category_weights: Record<string, number>;
};

type Props = {
  result: BacktestResult;
  categories: string[];
  exchangeByCompany: Map<number, string>;
  scoringConfig: ScoringConfig;
};

/** "Monthly Portfolios" card: one row per rebalance month, expandable to
 * show that month's holdings with per-stock returns and FX details. Owns
 * its own expansion state and the per-month turnover memo (which is only
 * read here). The parent feeds it the active backtest result; whenever
 * that changes — new run, loaded saved run, etc. — the table resets its
 * expansion automatically.
 */
export default function MonthlyHoldingsTable({ result, categories, exchangeByCompany, scoringConfig }: Props) {
  const [expandedMonth, setExpandedMonth] = useState<string | null>(null);
  // company_id whose timeline modal is open, or null for closed.
  const [timelineCompanyId, setTimelineCompanyId] = useState<number | null>(null);

  // One-way turnover per month: % of current holdings that weren't held
  // last month. First month has no prior portfolio → null.
  const turnoverByDate = useMemo<Record<string, number | null>>(() => {
    const map: Record<string, number | null> = {};
    let prevIds: Set<number> | null = null;
    for (const r of result.monthly_records) {
      const currIds = new Set(r.holdings.map((h) => h.company_id));
      if (prevIds === null || currIds.size === 0) {
        map[r.date] = null;
      } else {
        let added = 0;
        for (const id of currIds) if (!prevIds.has(id)) added += 1;
        map[r.date] = (added / currIds.size) * 100;
      }
      prevIds = currIds;
    }
    return map;
  }, [result]);

  // When the active result changes (new run / loaded saved run) collapse
  // any open month so the user starts at a clean view.
  useEffect(() => {
    setExpandedMonth(null);
  }, [result]);

  return (
    <>
    <CollapsibleCard title="Portfolios">
      <div className="max-h-[500px] overflow-auto border-t border-gray-800/40">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-[#151821] z-20">
            <tr className="text-gray-500 text-xs border-b border-gray-800/40">
              <th className="text-left px-5 py-2.5 font-medium">
                Period<CellInfoTip>Rebalance period start. The strategy enters this period&apos;s portfolio at the first trading day and holds until the next rebalance. Format is YYYY-MM for monthly+ cadences and YYYY-MM-DD for daily/weekly.</CellInfoTip>
              </th>
              <th className="text-right px-3 py-2.5 font-medium">
                Holdings<CellInfoTip>Number of stocks in the portfolio for this period (equal-weighted). Long-only: top_n_sectors × top_n_per_sector. Long-short: same on each side, so total is up to 2×.</CellInfoTip>
              </th>
              <th className="text-right px-3 py-2.5 font-medium">
                Return<CellInfoTip>Equal-weighted portfolio return for this period in EUR. Long-only: mean of holdings&apos; (exit ÷ entry) − 1. Long-short: long-side mean minus short-side mean.</CellInfoTip>
              </th>
              <th className="text-right px-3 py-2.5 font-medium">
                Turnover<CellInfoTip>Percentage of this period&apos;s holdings that weren&apos;t held in the previous period. 0% means the strategy held the same portfolio; 100% means it replaced everything.</CellInfoTip>
              </th>
              <th className="text-right px-5 py-2.5 font-medium">
                Cumulative<CellInfoTip>Cumulative return through the end of this period, since the backtest start: chain-linked product of all prior period returns.</CellInfoTip>
              </th>
            </tr>
          </thead>
          <tbody>
            {result.monthly_records.map((r) => (
              <Fragment key={r.date}>
                <tr
                  className="border-b border-gray-800/20 hover:bg-white/[0.02] cursor-pointer transition-colors"
                  onClick={() => setExpandedMonth(expandedMonth === r.date ? null : r.date)}
                >
                  <td className="px-5 py-2.5 text-gray-300 font-mono">
                    <span className="text-gray-600 mr-2">{expandedMonth === r.date ? '▾' : '▸'}</span>
                    {r.date}
                  </td>
                  <td className="text-right px-3 py-2.5 text-gray-400 font-mono">{r.holdings.length}</td>
                  <td className={`text-right px-3 py-2.5 font-mono ${r.portfolio_return_pct != null ? (r.portfolio_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                    {fmtPct(r.portfolio_return_pct)}
                  </td>
                  <td className="text-right px-3 py-2.5 font-mono text-gray-400">
                    {turnoverByDate[r.date] != null ? `${turnoverByDate[r.date]!.toFixed(1)}%` : '—'}
                  </td>
                  <td className={`text-right px-5 py-2.5 font-mono ${r.cumulative_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                    {fmtPct(r.cumulative_return_pct)}
                  </td>
                </tr>
                {expandedMonth === r.date && r.holdings.length > 0 && (
                  <tr key={`${r.date}-detail`}>
                    <td colSpan={5} className="bg-[#0f1117] px-5 py-3">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="text-gray-600">
                            <th className="text-left py-1 font-medium">
                              Side<CellInfoTip>Direction of the position. Long-only backtests are all &quot;Long&quot;. Long-short backtests group longs at the top and shorts at the bottom of each portfolio.</CellInfoTip>
                            </th>
                            <th className="text-left py-1 font-medium">
                              Ticker<CellInfoTip>The stock&apos;s ticker on its primary exchange. Click to open in GuruFocus.</CellInfoTip>
                            </th>
                            <th className="text-left py-1 font-medium">
                              Exchange<CellInfoTip>GuruFocus exchange code (e.g. NYSE, NASDAQ, HKSE, XTER). US-listed names use the bare ticker on GuruFocus; everything else is referenced as `EXCHANGE:TICKER`.</CellInfoTip>
                            </th>
                            <th className="text-left py-1 font-medium">
                              Company<CellInfoTip>Issuer name. Click to open in GuruFocus.</CellInfoTip>
                            </th>
                            <th className="text-left py-1 font-medium">
                              Sector<CellInfoTip>GICS sector. Selection picks top sectors then top stocks within each.</CellInfoTip>
                            </th>
                            {categories.map((cat) => (
                              <th key={cat} className="text-right py-1 font-medium">
                                {cat === 'price' ? 'Price' : cat === 'volume' ? 'Vol' : cat}
                                <CellInfoTip>
                                  {cat === 'price'
                                    ? 'Composite 0–100 score across the price-momentum signals, min-max normalized within the universe at this date.'
                                    : cat === 'volume'
                                    ? 'Composite 0–100 score across the volume signals, min-max normalized within the universe at this date.'
                                    : `${cat} category score, 0–100 normalized across the universe.`}
                                </CellInfoTip>
                              </th>
                            ))}
                            <th className="text-right py-1 font-medium">
                              Total<CellInfoTip>Weighted combination of the category scores. Selection ranks by this.</CellInfoTip>
                            </th>
                            <th className="text-right py-1 font-medium pl-4">
                              Start (local)<CellInfoTip>Entry price in local currency at the first trading day of this period.</CellInfoTip>
                            </th>
                            <th className="text-right py-1 font-medium">
                              End (local)<CellInfoTip>Exit price in local currency at the first trading day of the next period.</CellInfoTip>
                            </th>
                            <th className="text-right py-1 font-medium pl-4">
                              Start (€)<CellInfoTip>Entry price converted to EUR using the day&apos;s ECB FX rate.</CellInfoTip>
                            </th>
                            <th className="text-right py-1 font-medium">
                              End (€)<CellInfoTip>Exit price converted to EUR using the day&apos;s ECB FX rate.</CellInfoTip>
                            </th>
                            <th className="text-right py-1 font-medium pl-4">
                              Return<CellInfoTip>Per-stock price return in EUR over this period: (End € ÷ Start €) − 1. For shorts the period contribution is the negation of this.</CellInfoTip>
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          {[...r.holdings]
                            .sort((a, b) => {
                              // Longs first (top half), shorts second (bottom).
                              // Within each side, sort by sector then score so
                              // related names cluster together.
                              const sideA = a.side === 'short' ? 1 : 0;
                              const sideB = b.side === 'short' ? 1 : 0;
                              if (sideA !== sideB) return sideA - sideB;
                              const sec = a.sector.localeCompare(b.sector);
                              return sec !== 0 ? sec : b.score - a.score;
                            })
                            .map((h) => {
                              const exchRaw = exchangeByCompany.get(h.company_id) ?? '';
                              const exch = displayExchange(exchRaw, h.ticker);
                              const href = guruFocusUrl(h.ticker, exchRaw);
                              const isShort = h.side === 'short';
                              return (
                                <tr key={`${h.side ?? 'long'}-${h.company_id}`} className={`border-t border-gray-800/20 ${isShort ? 'bg-rose-500/[0.04]' : ''}`}>
                                  <td className="py-1.5 pr-2 whitespace-nowrap">
                                    <span
                                      className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium ${
                                        isShort
                                          ? 'bg-rose-500/15 text-rose-300 border border-rose-500/30'
                                          : 'bg-emerald-500/10 text-emerald-300 border border-emerald-500/25'
                                      }`}
                                    >
                                      {isShort ? 'Short' : 'Long'}
                                    </span>
                                  </td>
                                  <td className="py-1.5 font-mono whitespace-nowrap">
                                    <button
                                      type="button"
                                      onClick={() => setTimelineCompanyId(h.company_id)}
                                      className="mr-1.5 inline-flex w-3.5 h-3.5 items-center justify-center text-gray-500 hover:text-indigo-300 transition-colors align-middle"
                                      title={`Show ${h.ticker} holding history across the backtest`}
                                      aria-label={`Show ${h.ticker} timeline`}
                                    >
                                      <svg viewBox="0 0 16 16" fill="currentColor" className="w-3 h-3">
                                        <path d="M2 13h12v1H2v-1zm0-3h2v2H2v-2zm3-2h2v4H5V8zm3-3h2v7H8V5zm3-2h2v9h-2V3z" />
                                      </svg>
                                    </button>
                                    <a
                                      href={href}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="text-indigo-400 hover:text-indigo-300 hover:underline"
                                    >
                                      {h.ticker}
                                    </a>
                                    {exch && (
                                      <span
                                        className="ml-1 text-[10px] text-gray-500"
                                        title={EXCHANGE_NAMES[exch.toUpperCase()] ?? exch}
                                      >
                                        ({exch})
                                      </span>
                                    )}
                                  </td>
                                  <td
                                    className="py-1.5 text-gray-400 font-mono whitespace-nowrap"
                                    title={exch ? (EXCHANGE_NAMES[exch.toUpperCase()] ?? exch) : ''}
                                  >
                                    {exch || '—'}
                                  </td>
                                  <td className="py-1.5 truncate max-w-[200px]">
                                    <a
                                      href={href}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="text-gray-300 hover:text-indigo-300 hover:underline"
                                    >
                                      {h.company_name}
                                    </a>
                                  </td>
                                  <td className="py-1.5 text-gray-500">{h.sector}</td>
                                  {categories.map((cat) => (
                                    <td key={cat} className="text-right py-1.5 text-gray-400 font-mono">
                                      {h.category_scores?.[cat] != null ? h.category_scores[cat]!.toFixed(0) : '—'}
                                    </td>
                                  ))}
                                  <td className="text-right py-1.5 text-white font-mono font-medium">{h.score.toFixed(1)}</td>
                                  <td className="text-right py-1.5 text-gray-400 font-mono pl-4">
                                    {fmtPrice(h.entry_price_local)}
                                    {h.currency && <span className="text-gray-600 text-[10px] ml-1">{h.currency}</span>}
                                    {h.entry_date && (
                                      <CellInfoTip>
                                        <div className="text-gray-400">Trading date</div>
                                        <div className="font-mono text-gray-200">{h.entry_date}</div>
                                      </CellInfoTip>
                                    )}
                                  </td>
                                  <td className="text-right py-1.5 text-gray-400 font-mono">
                                    {fmtPrice(h.exit_price_local)}
                                    {h.exit_date && (
                                      <CellInfoTip>
                                        <div className="text-gray-400">Trading date</div>
                                        <div className="font-mono text-gray-200">{h.exit_date}</div>
                                      </CellInfoTip>
                                    )}
                                  </td>
                                  <td className="text-right py-1.5 text-gray-400 font-mono pl-4">
                                    {fmtPrice(h.entry_price_eur)}
                                    {(h.entry_date || (h.entry_price_eur != null && h.entry_price_local)) && (
                                      <CellInfoTip>
                                        {h.entry_date && (
                                          <>
                                            <div className="text-gray-400">Trading date</div>
                                            <div className="font-mono text-gray-200 mb-1">{h.entry_date}</div>
                                          </>
                                        )}
                                        {h.entry_price_eur != null && h.entry_price_local && h.entry_price_local > 0 && (
                                          <>
                                            <div className="text-gray-400">FX rate</div>
                                            <div className="font-mono text-gray-200">
                                              1 {h.currency ?? 'LCL'} = {(h.entry_price_eur / h.entry_price_local).toFixed(4)} EUR
                                            </div>
                                          </>
                                        )}
                                      </CellInfoTip>
                                    )}
                                  </td>
                                  <td className="text-right py-1.5 text-gray-400 font-mono">
                                    {fmtPrice(h.exit_price_eur)}
                                    {(h.exit_date || (h.exit_price_eur != null && h.exit_price_local)) && (
                                      <CellInfoTip>
                                        {h.exit_date && (
                                          <>
                                            <div className="text-gray-400">Trading date</div>
                                            <div className="font-mono text-gray-200 mb-1">{h.exit_date}</div>
                                          </>
                                        )}
                                        {h.exit_price_eur != null && h.exit_price_local && h.exit_price_local > 0 && (
                                          <>
                                            <div className="text-gray-400">FX rate</div>
                                            <div className="font-mono text-gray-200">
                                              1 {h.currency ?? 'LCL'} = {(h.exit_price_eur / h.exit_price_local).toFixed(4)} EUR
                                            </div>
                                          </>
                                        )}
                                      </CellInfoTip>
                                    )}
                                  </td>
                                  <td className={`text-right py-1.5 font-mono pl-4 ${h.forward_return_pct != null ? (h.forward_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                                    {fmtPct(h.forward_return_pct)}
                                  </td>
                                </tr>
                              );
                            })}
                        </tbody>
                      </table>
                    </td>
                  </tr>
                )}
                {expandedMonth === r.date && r.holdings.length === 0 && (
                  <tr key={`${r.date}-empty`}>
                    <td colSpan={4} className="bg-[#0f1117] px-5 py-4">
                      <div className="text-xs text-gray-500">
                        {r.empty_reason || 'No holdings for this period (unknown reason)'}
                      </div>
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </CollapsibleCard>
    <TickerTimelineModal
      result={result}
      companyId={timelineCompanyId}
      exchangeByCompany={exchangeByCompany}
      scoringConfig={scoringConfig}
      onClose={() => setTimelineCompanyId(null)}
    />
    </>
  );
}
