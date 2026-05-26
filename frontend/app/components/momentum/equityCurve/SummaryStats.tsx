'use client';

import type { BacktestResult } from '../../../../lib/stores/momentum';
import type { Column } from '../../../../lib/tableExport';
import CellInfoTip from '../CellInfoTip';
import CollapsibleCard from '../CollapsibleCard';
import TableDownloadButton from '../../TableDownloadButton';
import { fmtPct } from '../utils';
import { parenPct, type NetStats } from '../feeStats';
import type { AlignedResult, AlignedSeries } from './seriesMath';

/** Summary stats table (one row per aligned series), strategy-level
 * raw metrics, optional cross-trial sampling stats, and per-series top-3
 * drawdowns. Pure presentation — every value is passed in. */
export default function SummaryStats({
  result,
  alignedSeries,
  activeNetStats,
  comparisonNetStats,
  summaryExportColumns,
}: {
  result: BacktestResult;
  alignedSeries: AlignedResult;
  activeNetStats: NetStats | null;
  comparisonNetStats: Map<string, NetStats | null>;
  summaryExportColumns: Column<AlignedSeries>[];
}) {
  return (
    <CollapsibleCard
      title="Summary"
      rightSlot={
        <TableDownloadButton
          rows={alignedSeries.series}
          columns={summaryExportColumns}
          filename="backtest_summary"
          title={`Download ${alignedSeries.series.length} series as CSV / XLSX`}
        />
      }
    >
      <table className="w-full text-sm border-t border-gray-800/40">
        <thead>
          <tr className="border-b border-gray-800/40 text-gray-500 text-xs">
            <th className="px-4 py-2.5 text-left font-medium"></th>
            <th className="px-3 py-2.5 text-right font-medium">
              Total Return<CellInfoTip>Cumulative return over the entire backtest period: (1 + r₁)(1 + r₂)…(1 + rₙ) − 1.</CellInfoTip>
            </th>
            <th className="px-3 py-2.5 text-right font-medium">
              Annualized<CellInfoTip>Geometric annual return: (1 + total_return)^(1/years) − 1. Years are derived from the actual span of dates in the curve, not the period count.</CellInfoTip>
            </th>
            <th className="px-3 py-2.5 text-right font-medium">
              Max Drawdown<CellInfoTip>Largest peak-to-trough decline observed during the backtest, expressed as a negative percentage of the prior peak. Computed daily when the strategy ships a daily curve (so intra-month moves on a monthly strategy are caught), monthly otherwise.</CellInfoTip>
            </th>
            <th className="px-3 py-2.5 text-right font-medium">
              Sharpe<CellInfoTip>Annualized Sharpe ratio of period returns (risk-free rate = 0): mean ÷ std × √(periods/year). Auto-detects cadence — daily curves use √252, monthly √12. Computed only when at least one full year of observations is available.</CellInfoTip>
            </th>
            <th className="px-3 py-2.5 text-right font-medium">
              Periods<CellInfoTip>Number of return observations in the aligned window. Equals trading days when the curve is daily, calendar months when the curve is monthly.</CellInfoTip>
            </th>
          </tr>
        </thead>
        <tbody>
          {alignedSeries.series.map((s) => {
            // The (net) parenthetical applies to the active strategy AND
            // every saved comparison — both have holdings whose
            // exchanges we can resolve via `exchangeByCompany`.
            // Benchmarks (kind === 'benchmark') have no holdings to
            // trade so they always render gross-only.
            const net = s.kind === 'active'
              ? activeNetStats
              : s.kind === 'saved'
                ? comparisonNetStats.get(s.id) ?? null
                : null;
            return (
              <tr key={s.id} className="border-b border-gray-800/30">
                <td className="px-4 py-2.5 font-medium flex items-center gap-2">
                  <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
                  <span className="text-gray-200">{s.label}</span>
                </td>
                <td className={`px-3 py-2.5 text-right font-mono ${s.stats.totalReturn >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(s.stats.totalReturn)}<span className="text-gray-500">{parenPct(net?.total_return_pct)}</span></td>
                <td className={`px-3 py-2.5 text-right font-mono ${s.stats.annualized >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(s.stats.annualized)}<span className="text-gray-500">{parenPct(net?.annualized_return_pct)}</span></td>
                <td className="px-3 py-2.5 text-right font-mono text-rose-400">{fmtPct(s.stats.maxDd)}<span className="text-gray-500">{parenPct(net?.max_drawdown_pct)}</span></td>
                <td className="px-3 py-2.5 text-right font-mono text-white">{s.stats.sharpe != null ? s.stats.sharpe.toFixed(2) : '—'}<span className="text-gray-500">{net?.sharpe_ratio != null ? ` (${net.sharpe_ratio.toFixed(2)})` : ''}</span></td>
                <td className="px-3 py-2.5 text-right font-mono text-gray-300">{s.stats.months}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {/* Active strategy — raw (non-aligned) metrics. The Sortino / win
          rate / median row complements the table by showing risk-adjusted
          stats the aligned per-series table doesn't carry (those are
          active-strategy-only — benchmarks have no per-period returns
          we can win-rate against). */}
      <div className="px-4 py-3 border-t border-gray-800/40 text-xs text-gray-500 flex flex-wrap gap-x-6 gap-y-1">
        <span>Strategy (full range): <span className="font-mono text-gray-300">Turnover {fmtPct(result.summary.avg_monthly_turnover_pct)}</span></span>
        <span><span className="font-mono text-gray-300">Avg Holdings {result.summary.avg_holdings.toFixed(1)}</span></span>
        <span><span className="font-mono text-gray-300">Months {result.summary.total_months}</span></span>
        {result.summary.sortino_ratio != null && (
          <span title="Sortino: like Sharpe but only penalizes downside vol (std of negative daily returns × √252). Higher than Sharpe → upside vol dominates.">
            <span className="font-mono text-gray-300">Sortino {result.summary.sortino_ratio.toFixed(2)}</span>
          </span>
        )}
        {result.summary.win_rate_pct != null && (
          <span title="% of calendar months with strictly positive return — computed from the daily equity curve resampled to month-end, regardless of rebalance cadence.">
            <span className="font-mono text-gray-300">Win rate {result.summary.win_rate_pct.toFixed(0)}%</span>
          </span>
        )}
        {result.summary.median_period_return_pct != null && (
          <span title="Median calendar-month return — computed from the daily equity curve resampled to month-end, regardless of rebalance cadence. Far below the headline mean → the strategy's return is carried by a few outlier months rather than steady ones.">
            <span className={`font-mono ${result.summary.median_period_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>Median month {fmtPct(result.summary.median_period_return_pct)}</span>
          </span>
        )}
      </div>
      {/* Universe baseline ("hold the entire eligible universe equal-
          weight"). Same period-chain over closed periods as the
          strategy, so alpha = strategy total − universe total. The
          row is hidden when the engine produced no universe baseline
          (degenerate run / legacy result without the field). */}
      {result.summary.universe_total_return_pct != null && (
        <div className="px-4 py-3 border-t border-gray-800/40 text-xs text-gray-500 flex flex-wrap gap-x-6 gap-y-1">
          <span>Universe baseline: <span className={`font-mono ${result.summary.universe_total_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(result.summary.universe_total_return_pct)}</span><span className="text-gray-600"> total</span></span>
          {result.summary.universe_annualized_return_pct != null && (
            <span><span className={`font-mono ${result.summary.universe_annualized_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(result.summary.universe_annualized_return_pct)}</span><span className="text-gray-600"> annualized</span></span>
          )}
          {(() => {
            const alpha = result.summary.total_return_pct - result.summary.universe_total_return_pct;
            return (
              <span title="Strategy total return minus universe total return — positive means the picks added value on top of just being in the market.">
                <span className="text-gray-600">Alpha vs. universe: </span>
                <span className={`font-mono font-medium ${alpha >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                  {alpha >= 0 ? '+' : ''}{alpha.toFixed(2)}%
                </span>
              </span>
            );
          })()}
        </div>
      )}
      {/* Multi-trial cross-trial statistics — backend means ± std. These
          are the numbers to compare a momentum run against, NOT the
          per-series stats above (which derive from the mean equity curve
          and understate volatility). */}
      {result.summary.n_trials != null && result.summary.n_trials > 1 && (
        <div className="px-4 py-3 border-t border-gray-800/40">
          <div className="text-xs font-medium text-gray-400 mb-2">
            Cross-trial statistics ({result.summary.n_trials} random trials, mean ± std)
          </div>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3 text-xs">
            <div className="bg-[#0f1117] rounded-lg px-3 py-2">
              <div className="text-gray-500">Total Return</div>
              <div className="font-mono text-gray-200">
                {fmtPct(result.summary.total_return_pct)}
                <span className="text-gray-500"> ± {(result.summary.total_return_pct_std ?? 0).toFixed(2)}%</span>
              </div>
            </div>
            <div className="bg-[#0f1117] rounded-lg px-3 py-2">
              <div className="text-gray-500">Annualized</div>
              <div className="font-mono text-gray-200">
                {fmtPct(result.summary.annualized_return_pct)}
                <span className="text-gray-500"> ± {(result.summary.annualized_return_pct_std ?? 0).toFixed(2)}%</span>
              </div>
            </div>
            <div className="bg-[#0f1117] rounded-lg px-3 py-2">
              <div className="text-gray-500">Max Drawdown</div>
              <div className="font-mono text-gray-200">
                {fmtPct(result.summary.max_drawdown_pct)}
                <span className="text-gray-500"> ± {(result.summary.max_drawdown_pct_std ?? 0).toFixed(2)}%</span>
              </div>
            </div>
            <div className="bg-[#0f1117] rounded-lg px-3 py-2">
              <div className="text-gray-500">Sharpe</div>
              <div className="font-mono text-gray-200">
                {result.summary.sharpe_ratio != null ? result.summary.sharpe_ratio.toFixed(2) : '—'}
                {result.summary.sharpe_ratio_std != null && (
                  <span className="text-gray-500"> ± {result.summary.sharpe_ratio_std.toFixed(2)}</span>
                )}
              </div>
            </div>
            <div className="bg-[#0f1117] rounded-lg px-3 py-2">
              <div className="text-gray-500">Turnover</div>
              <div className="font-mono text-gray-200">
                {fmtPct(result.summary.avg_monthly_turnover_pct)}
                <span className="text-gray-500"> ± {(result.summary.avg_monthly_turnover_pct_std ?? 0).toFixed(2)}%</span>
              </div>
            </div>
          </div>
        </div>
      )}
      {alignedSeries.series.some((s) => s.topDrawdowns.length > 0) && (
        <div className="px-4 py-3 border-t border-gray-800/40 space-y-3">
          {alignedSeries.series.map((s) => (
            s.topDrawdowns.length > 0 && (
              <div key={s.id}>
                <div className="text-xs font-medium mb-2 flex items-center gap-2">
                  <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
                  <span className="text-gray-400">{s.label} — Top Drawdowns</span>
                </div>
                <div className="grid grid-cols-3 gap-3">
                  {s.topDrawdowns.map((dd, i) => {
                    const alpha = [1.0, 0.6, 0.3][i] ?? 0.3;
                    return (
                      <div key={i} className="bg-[#0f1117] rounded-lg px-3 py-2">
                        <div className="flex items-center gap-2 mb-1">
                          <div className="w-2 h-2 rounded-full" style={{ background: s.color, opacity: alpha }} />
                          <span className="font-mono text-sm font-medium" style={{ color: s.color }}>{dd.drawdown_pct.toFixed(1)}%</span>
                        </div>
                        <div className="text-[10px] text-gray-500 font-mono">
                          {dd.peak_date} to {dd.trough_date}
                          {dd.recovery_date ? ` (recovered ${dd.recovery_date})` : ' (ongoing)'}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )
          ))}
        </div>
      )}
    </CollapsibleCard>
  );
}
