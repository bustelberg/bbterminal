'use client';

import { Fragment, useMemo, useState } from 'react';
import type { CurrentPortfolio, DailyPick } from '../../../lib/stores/momentum';
import CellInfoTip from './CellInfoTip';
import { EXCHANGE_NAMES, fmtPct, fmtPrice, guruFocusUrl } from './utils';

type Props = {
  currentPortfolio: CurrentPortfolio | null;
  categories: string[];
  exchangeByCompany: Map<number, string>;
};

/** Cross-month "Daily picks history" card under Current Picks. Months are
 * expandable rows; each month expands to its stored days; each day expands
 * to full per-holding detail. Past months are read-only — only days
 * already saved are shown. The card returns null when there's nothing to
 * show, so callers can render it unconditionally. */
export default function DailyPicksHistory({ currentPortfolio, categories, exchangeByCompany }: Props) {
  const [expandedHistoryMonth, setExpandedHistoryMonth] = useState<string | null>(null);
  const [expandedDailyDate, setExpandedDailyDate] = useState<string | null>(null);

  // Group all stored daily picks by YYYY-MM. Falls back to daily_picks for
  // legacy snapshots that predate daily_picks_history.
  const dailyPicksByMonth = useMemo<Array<{ month: string; days: DailyPick[] }>>(() => {
    if (!currentPortfolio) return [];
    const source = currentPortfolio.daily_picks_history && currentPortfolio.daily_picks_history.length > 0
      ? currentPortfolio.daily_picks_history
      : currentPortfolio.daily_picks ?? [];
    const groups = new Map<string, DailyPick[]>();
    for (const dp of source) {
      const month = dp.date.slice(0, 7);
      const arr = groups.get(month);
      if (arr) arr.push(dp);
      else groups.set(month, [dp]);
    }
    // Sort months descending (most recent first); days within ascending.
    return Array.from(groups.entries())
      .sort((a, b) => b[0].localeCompare(a[0]))
      .map(([month, days]) => ({
        month,
        days: days.slice().sort((a, b) => a.date.localeCompare(b.date)),
      }));
  }, [currentPortfolio]);

  if (dailyPicksByMonth.length === 0) return null;

  return (
    <div className="border-t border-gray-800/40">
      <div className="px-4 py-3 border-b border-gray-800/40">
        <div className="text-sm font-medium text-white">Daily picks history</div>
        <div className="text-xs text-gray-500 mt-0.5">
          Hypothetical: what the strategy would pick if rebalancing on each day. <span className="text-gray-400">MTD (chain)</span> is the cumulative return through that day, chain-linked across rebalances. <span className="text-gray-400">Next day</span> is that day&apos;s portfolio held one trading day forward. Past months are read-only — only days already saved are shown.
        </div>
      </div>
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-gray-800/40 text-gray-600">
            <th className="text-left px-4 py-2 font-medium">
              Month
              <CellInfoTip>Calendar month (YYYY-MM) for which daily picks are stored. Past months are read-only — only the days already saved are listed.</CellInfoTip>
            </th>
            <th className="text-right px-3 py-2 font-medium">
              Days
              <CellInfoTip>Number of trading days saved for this month.</CellInfoTip>
            </th>
            <th className="text-right px-3 py-2 font-medium">
              Latest MTD
              <CellInfoTip>Chain-linked cumulative MTD return through the latest stored day in this month.</CellInfoTip>
            </th>
            <th className="text-right px-3 py-2 font-medium" colSpan={2} />
          </tr>
        </thead>
        <tbody>
          {dailyPicksByMonth.map(({ month, days }) => {
            const latestDay = days[days.length - 1];
            const latestMTD = latestDay?.portfolio_return_pct ?? null;
            const isOpen = expandedHistoryMonth === month;
            const monthLabel = (() => {
              const [y, m] = month.split('-').map(Number);
              return new Date(y, m - 1).toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
            })();
            return (
              <Fragment key={month}>
                <tr
                  className="border-b border-gray-800/40 hover:bg-white/[0.02] cursor-pointer"
                  onClick={() => setExpandedHistoryMonth(isOpen ? null : month)}
                >
                  <td className="px-4 py-2 font-mono text-gray-200">
                    <span className="text-gray-600 mr-2">{isOpen ? '▾' : '▸'}</span>
                    {monthLabel}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-gray-300">{days.length}</td>
                  <td className={`px-3 py-2 text-right font-mono ${latestMTD != null ? (latestMTD >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                    {fmtPct(latestMTD)}
                  </td>
                  <td colSpan={2} />
                </tr>
                {isOpen && (
                  <tr>
                    <td colSpan={5} className="bg-[#0f1117] p-0">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="border-b border-gray-800/30 text-gray-600">
                            <th className="text-left px-5 py-1.5 font-medium">
                              Date
                              <CellInfoTip>The trading day on which the strategy is evaluated. Picks shown are what the strategy would buy at this day&apos;s close.</CellInfoTip>
                            </th>
                            <th className="text-right px-3 py-1.5 font-medium">
                              Holdings
                              <CellInfoTip>Number of stocks in this day&apos;s portfolio (equal-weighted).</CellInfoTip>
                            </th>
                            <th className="text-right px-3 py-1.5 font-medium">
                              MTD (chain)
                              <CellInfoTip>Cumulative month-to-date return through this day, chain-linked across rebalances. Each day&apos;s contribution = the prior day&apos;s portfolio held one trading day forward. Daily returns are multiplied: (1+r₁)(1+r₂)…(1+rₙ) − 1.</CellInfoTip>
                            </th>
                            <th className="text-right px-3 py-1.5 font-medium">
                              Next day
                              <CellInfoTip>One-day forward return of THIS day&apos;s portfolio held to the next trading day&apos;s close. Empty on the latest day (no next trading day yet).</CellInfoTip>
                            </th>
                            <th className="text-right px-3 py-1.5 font-medium">
                              Turnover
                              <CellInfoTip>Number of stocks that differ from the previous day&apos;s portfolio. For a fixed-size portfolio this equals both stocks added and stocks removed.</CellInfoTip>
                            </th>
                            <th className="text-right px-3 py-1.5 font-medium">
                              %
                              <CellInfoTip>Turnover expressed as a percentage of portfolio size: turnover ÷ max(today, yesterday) × 100.</CellInfoTip>
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          {days.map((dp) => (
                            <Fragment key={dp.date}>
                              <tr
                                className="border-b border-gray-800/30 hover:bg-white/[0.02] cursor-pointer"
                                onClick={() => setExpandedDailyDate(expandedDailyDate === dp.date ? null : dp.date)}
                              >
                                <td className="px-4 py-1.5 font-mono text-gray-200">
                                  <span className="text-gray-600 mr-2">{expandedDailyDate === dp.date ? '▾' : '▸'}</span>
                                  {dp.date}
                                </td>
                                <td className="px-3 py-1.5 text-right font-mono text-gray-300">{dp.holdings.length}</td>
                                <td className={`px-3 py-1.5 text-right font-mono ${dp.portfolio_return_pct != null ? (dp.portfolio_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                                  {fmtPct(dp.portfolio_return_pct ?? null)}
                                </td>
                                <td className={`px-3 py-1.5 text-right font-mono ${dp.next_day_return_pct != null ? (dp.next_day_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                                  {fmtPct(dp.next_day_return_pct ?? null)}
                                </td>
                                <td className={`px-3 py-1.5 text-right font-mono ${dp.turnover_abs > 0 ? 'text-amber-400' : 'text-gray-600'}`}>
                                  {dp.turnover_abs > 0 ? dp.turnover_abs : '—'}
                                </td>
                                <td className={`px-3 py-1.5 text-right font-mono ${dp.turnover_pct > 0 ? 'text-amber-400' : 'text-gray-600'}`}>
                                  {dp.turnover_pct > 0 ? `${dp.turnover_pct.toFixed(2)}%` : '—'}
                                </td>
                              </tr>
                              {expandedDailyDate === dp.date && (
                                <tr>
                                  <td colSpan={6} className="bg-[#0f1117] px-5 py-3">
                                    <table className="w-full text-xs">
                                      <thead>
                                        <tr className="text-gray-600">
                                          <th className="text-left py-1 font-medium">
                                            Ticker<CellInfoTip>The stock&apos;s ticker on its primary exchange. Click to open in GuruFocus.</CellInfoTip>
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
                                            Start (local)<CellInfoTip>Entry price in local currency at this day&apos;s close — each day&apos;s pick is an independent 1-day portfolio.</CellInfoTip>
                                          </th>
                                          <th className="text-right py-1 font-medium">
                                            End (local)<CellInfoTip>Exit price in local currency at the next trading day&apos;s close. Empty on the latest day (no next trading day yet).</CellInfoTip>
                                          </th>
                                          <th className="text-right py-1 font-medium pl-4">
                                            Start (€)<CellInfoTip>Entry price converted to EUR using the day&apos;s ECB FX rate.</CellInfoTip>
                                          </th>
                                          <th className="text-right py-1 font-medium">
                                            End (€)<CellInfoTip>Exit price converted to EUR using the next trading day&apos;s ECB FX rate.</CellInfoTip>
                                          </th>
                                          <th className="text-right py-1 font-medium pl-4">
                                            Return<CellInfoTip>Per-stock 1-day return in EUR: (End € ÷ Start €) − 1. Empty on the latest day.</CellInfoTip>
                                          </th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {[...dp.holdings]
                                          .sort((a, b) => {
                                            const sec = a.sector.localeCompare(b.sector);
                                            return sec !== 0 ? sec : b.score - a.score;
                                          })
                                          .map((h) => {
                                            const exch = exchangeByCompany.get(h.company_id) ?? '';
                                            const href = guruFocusUrl(h.ticker, exch);
                                            return (
                                              <tr key={h.company_id} className="border-t border-gray-800/20">
                                                <td className="py-1.5 font-mono whitespace-nowrap">
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
                                                  {fmtPrice(h.entry_price_local ?? null)}
                                                  {h.currency && <span className="text-gray-600 text-[10px] ml-1">{h.currency}</span>}
                                                  {h.entry_date && (
                                                    <CellInfoTip>
                                                      <div className="text-gray-400">Trading date</div>
                                                      <div className="font-mono text-gray-200">{h.entry_date}</div>
                                                    </CellInfoTip>
                                                  )}
                                                </td>
                                                <td className="text-right py-1.5 text-gray-400 font-mono">
                                                  {fmtPrice(h.exit_price_local ?? null)}
                                                  {h.exit_date && (
                                                    <CellInfoTip>
                                                      <div className="text-gray-400">Trading date</div>
                                                      <div className="font-mono text-gray-200">{h.exit_date}</div>
                                                    </CellInfoTip>
                                                  )}
                                                </td>
                                                <td className="text-right py-1.5 text-gray-400 font-mono pl-4">
                                                  {fmtPrice(h.entry_price_eur ?? null)}
                                                </td>
                                                <td className="text-right py-1.5 text-gray-400 font-mono">
                                                  {fmtPrice(h.exit_price_eur ?? null)}
                                                </td>
                                                <td className={`text-right py-1.5 font-mono pl-4 ${h.forward_return_pct != null ? (h.forward_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                                                  {fmtPct(h.forward_return_pct ?? null)}
                                                </td>
                                              </tr>
                                            );
                                          })}
                                      </tbody>
                                    </table>
                                  </td>
                                </tr>
                              )}
                            </Fragment>
                          ))}
                        </tbody>
                      </table>
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
