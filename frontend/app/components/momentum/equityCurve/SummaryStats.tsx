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
      <table className="w-full text-sm border-t border-neutral-800/40">
        <thead>
          <tr className="border-b border-neutral-800/40 text-fg-subtle text-xs">
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
              <tr key={s.id} className="border-b border-neutral-800/30">
                <td className="px-4 py-2.5 font-medium flex items-center gap-2">
                  <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
                  <span className="text-fg">{s.label}</span>
                </td>
                <td className={`px-3 py-2.5 text-right font-mono ${s.stats.totalReturn >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>{fmtPct(s.stats.totalReturn)}<span className="text-fg-subtle">{parenPct(net?.total_return_pct)}</span></td>
                <td className={`px-3 py-2.5 text-right font-mono ${s.stats.annualized >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>{fmtPct(s.stats.annualized)}<span className="text-fg-subtle">{parenPct(net?.annualized_return_pct)}</span></td>
                <td className="px-3 py-2.5 text-right font-mono text-neg-400">{fmtPct(s.stats.maxDd)}<span className="text-fg-subtle">{parenPct(net?.max_drawdown_pct)}</span></td>
                <td className="px-3 py-2.5 text-right font-mono text-fg-strong">{s.stats.sharpe != null ? s.stats.sharpe.toFixed(2) : '—'}<span className="text-fg-subtle">{net?.sharpe_ratio != null ? ` (${net.sharpe_ratio.toFixed(2)})` : ''}</span></td>
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
      <div className="px-4 py-3 border-t border-neutral-800/40 text-xs text-fg-subtle flex flex-wrap gap-x-6 gap-y-1">
        <span>Strategy (full range): <span className="font-mono text-fg-soft">Turnover {fmtPct(result.summary.avg_monthly_turnover_pct)}</span></span>
        <span><span className="font-mono text-fg-soft">Avg Holdings {result.summary.avg_holdings.toFixed(1)}</span></span>
        <span><span className="font-mono text-fg-soft">Months {result.summary.total_months}</span></span>
        {result.summary.sortino_ratio != null && (
          <span title="Sortino: like Sharpe but only penalizes downside vol (std of negative daily returns × √252). Higher than Sharpe → upside vol dominates.">
            <span className="font-mono text-fg-soft">Sortino {result.summary.sortino_ratio.toFixed(2)}</span>
          </span>
        )}
        {result.summary.win_rate_pct != null && (
          <span title="% of calendar months with strictly positive return — computed from the daily equity curve resampled to month-end, regardless of rebalance cadence.">
            <span className="font-mono text-fg-soft">Win rate {result.summary.win_rate_pct.toFixed(0)}%</span>
          </span>
        )}
        {/* Alpha vs. the universe — derived from the SAME aligned series the
            table above shows (active total − universe total), so the number
            is consistent with the visible "Universe (equal-weight)" row
            rather than introducing a second, slightly-different baseline. */}
        {(() => {
          const strat = alignedSeries.series.find((s) => s.kind === 'active');
          const uni = alignedSeries.series.find((s) => s.id === 'universe');
          if (!strat || !uni) return null;
          const alpha = strat.stats.totalReturn - uni.stats.totalReturn;
          return (
            <span title="Strategy total return minus the equal-weight universe's, over the same window — positive means the picks added value beyond simply being in the market.">
              <span className="text-fg-faint">Alpha </span>
              <span className={`font-mono font-medium ${alpha >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>
                {alpha >= 0 ? '+' : ''}{alpha.toFixed(2)}%
              </span>
            </span>
          );
        })()}
      </div>
      {/* Multi-trial cross-trial statistics — backend means ± std. These
          are the numbers to compare a momentum run against, NOT the
          per-series stats above (which derive from the mean equity curve
          and understate volatility). */}
      {result.summary.n_trials != null && result.summary.n_trials > 1 && (
        <div className="px-4 py-3 border-t border-neutral-800/40">
          <div className="text-xs font-medium text-fg-muted mb-2">
            Cross-trial statistics ({result.summary.n_trials} random trials, mean ± std)
          </div>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3 text-xs">
            <div className="bg-page rounded-lg px-3 py-2">
              <div className="text-fg-subtle">Total Return</div>
              <div className="font-mono text-fg">
                {fmtPct(result.summary.total_return_pct)}
                <span className="text-fg-subtle"> ± {(result.summary.total_return_pct_std ?? 0).toFixed(2)}%</span>
              </div>
            </div>
            <div className="bg-page rounded-lg px-3 py-2">
              <div className="text-fg-subtle">Annualized</div>
              <div className="font-mono text-fg">
                {fmtPct(result.summary.annualized_return_pct)}
                <span className="text-fg-subtle"> ± {(result.summary.annualized_return_pct_std ?? 0).toFixed(2)}%</span>
              </div>
            </div>
            <div className="bg-page rounded-lg px-3 py-2">
              <div className="text-fg-subtle">Max Drawdown</div>
              <div className="font-mono text-fg">
                {fmtPct(result.summary.max_drawdown_pct)}
                <span className="text-fg-subtle"> ± {(result.summary.max_drawdown_pct_std ?? 0).toFixed(2)}%</span>
              </div>
            </div>
            <div className="bg-page rounded-lg px-3 py-2">
              <div className="text-fg-subtle">Sharpe</div>
              <div className="font-mono text-fg">
                {result.summary.sharpe_ratio != null ? result.summary.sharpe_ratio.toFixed(2) : '—'}
                {result.summary.sharpe_ratio_std != null && (
                  <span className="text-fg-subtle"> ± {result.summary.sharpe_ratio_std.toFixed(2)}</span>
                )}
              </div>
            </div>
            <div className="bg-page rounded-lg px-3 py-2">
              <div className="text-fg-subtle">Turnover</div>
              <div className="font-mono text-fg">
                {fmtPct(result.summary.avg_monthly_turnover_pct)}
                <span className="text-fg-subtle"> ± {(result.summary.avg_monthly_turnover_pct_std ?? 0).toFixed(2)}%</span>
              </div>
            </div>
          </div>
        </div>
      )}
      {/* Strategy (+ saved comparison) top drawdowns only — the universe
          baseline's Max Drawdown is already in the table above; its full
          top-3 list is just clutter. */}
      {alignedSeries.series.some((s) => s.kind !== 'benchmark' && s.topDrawdowns.length > 0) && (
        <div className="px-4 py-3 border-t border-neutral-800/40 space-y-3">
          {alignedSeries.series.filter((s) => s.kind !== 'benchmark').map((s) => (
            s.topDrawdowns.length > 0 && (
              <div key={s.id}>
                <div className="text-xs font-medium mb-2 flex items-center gap-2">
                  <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
                  <span className="text-fg-muted">{s.label} — Top Drawdowns</span>
                </div>
                <div className="grid grid-cols-3 gap-3">
                  {s.topDrawdowns.map((dd, i) => {
                    const alpha = [1.0, 0.6, 0.3][i] ?? 0.3;
                    return (
                      <div key={i} className="bg-page rounded-lg px-3 py-2">
                        <div className="flex items-center gap-2 mb-1">
                          <div className="w-2 h-2 rounded-full" style={{ background: s.color, opacity: alpha }} />
                          <span className="font-mono text-sm font-medium" style={{ color: s.color }}>{dd.drawdown_pct.toFixed(1)}%</span>
                        </div>
                        <div className="text-[10px] text-fg-subtle font-mono">
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
