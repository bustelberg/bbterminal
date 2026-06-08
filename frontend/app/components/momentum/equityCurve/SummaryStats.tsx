'use client';

import { useMemo } from 'react';
import type { ReactNode } from 'react';
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
  // Active-risk stats vs the universe: tracking error (annualised std of the
  // daily strategy − universe active return) and information ratio
  // (annualised active return ÷ tracking error). Computed from the daily
  // curves so they're consistent with the chart.
  const active = useMemo(() => {
    const dailyRet = (recs: { date: string; cumulative_return_pct: number }[]) => {
      const out = new Map<string, number>();
      for (let i = 1; i < recs.length; i++) {
        const f0 = 1 + recs[i - 1].cumulative_return_pct / 100;
        const f1 = 1 + recs[i].cumulative_return_pct / 100;
        if (f0 > 0) out.set(recs[i].date.slice(0, 10), (f1 / f0 - 1) * 100);
      }
      return out;
    };
    const sd = dailyRet(result.daily_records ?? []);
    const ud = dailyRet(result.universe_daily_records ?? []);
    const diffs: number[] = [];
    for (const [d, sr] of sd) {
      const ur = ud.get(d);
      if (ur !== undefined) diffs.push(sr - ur);
    }
    const n = diffs.length;
    if (n < 21) return null;
    const mean = diffs.reduce((a, b) => a + b, 0) / n;
    const dev = Math.sqrt(diffs.reduce((a, b) => a + (b - mean) ** 2, 0) / n);
    if (dev <= 0) return null;
    return { te: dev * Math.sqrt(252), ir: (mean / dev) * Math.sqrt(252) };
  }, [result]);

  // Calmar — annualised return per unit of max drawdown.
  const dd = Math.abs(result.summary.max_drawdown_pct);
  const calmar = dd > 0 ? result.summary.annualized_return_pct / dd : null;

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
      <div className="px-4 py-3 border-t border-neutral-800/40">
        <div className="grid grid-cols-3 sm:grid-cols-6 gap-x-4 gap-y-3">
          {/* Alpha — derived from the SAME aligned series the table shows
              (active total − universe total), consistent with the visible
              "Universe (equal-weight)" row. */}
          {(() => {
            const strat = alignedSeries.series.find((s) => s.kind === 'active');
            const uni = alignedSeries.series.find((s) => s.id === 'universe');
            if (!strat || !uni) return null;
            const alpha = strat.stats.totalReturn - uni.stats.totalReturn;
            return (
              <Stat
                label="Alpha"
                tone={alpha >= 0 ? 'text-pos-400' : 'text-neg-400'}
                info="Strategy total return minus the equal-weight universe's, over the same window. Positive (green) = the picks beat simply holding the whole eligible universe."
              >
                {alpha >= 0 ? '+' : ''}{alpha.toFixed(2)}%
              </Stat>
            );
          })()}
          {active != null && (
            <Stat
              label="Info ratio"
              tone={toneClass(active.ir, 0.5, 0)}
              info={`Annualised active return (strategy − universe) ÷ tracking error — risk-adjusted skill vs the universe. Rough scale: ≈0.5 good, >1 excellent, <0 means the active bets hurt. (Tracking error ${active.te.toFixed(1)}%/yr.)`}
            >
              {active.ir.toFixed(2)}
            </Stat>
          )}
          {active != null && (
            <Stat
              label="Tracking err"
              info="Annualised standard deviation of the daily active return (strategy − universe) — how far the strategy wanders from the universe. Not good or bad on its own; it's the denominator of the information ratio."
            >
              {active.te.toFixed(1)}%
            </Stat>
          )}
          {calmar != null && (
            <Stat
              label="Calmar"
              tone={toneClass(calmar, 1, 0)}
              info="Annualised return ÷ |max drawdown| — reward earned per unit of worst-case pain. >1 is solid; higher is better; <0 means the strategy lost money over the window."
            >
              {calmar.toFixed(2)}
            </Stat>
          )}
          {result.summary.win_rate_pct != null && (
            <Stat
              label="Win rate"
              tone={toneClass(result.summary.win_rate_pct, 55, 45)}
              info="Share of calendar months with a strictly positive return (daily curve resampled to month-end). >50% = more up months than down. Note: a high win rate can still pair with poor returns if the losing months are large."
            >
              {result.summary.win_rate_pct.toFixed(0)}%
            </Stat>
          )}
          {result.summary.sortino_ratio != null && (
            <Stat
              label="Sortino"
              tone={toneClass(result.summary.sortino_ratio, 1.5, 0)}
              info="Like Sharpe but penalises only downside volatility (std of negative daily returns × √252). Higher = better risk-adjusted return; upside swings aren't punished. Rough scale: >1 good, >2 excellent."
            >
              {result.summary.sortino_ratio.toFixed(2)}
            </Stat>
          )}
          <Stat
            label="Turnover"
            info="Average month-over-month change in holdings (100% = the whole book is replaced each rebalance). Higher means more trading and more fee drag — see the net figures in the table above."
          >
            {fmtPct(result.summary.avg_monthly_turnover_pct)}
          </Stat>
          <Stat label="Avg holdings" info="Average number of positions held per rebalance period.">
            {result.summary.avg_holdings.toFixed(1)}
          </Stat>
          <Stat label="Months" info="Number of months in the backtest window — the sample size behind these statistics. More months = more reliable.">
            {result.summary.total_months}
          </Stat>
        </div>
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
                <div className="text-[11px] font-medium mb-2 flex items-center gap-2">
                  <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
                  <span className="text-fg-muted uppercase tracking-wide">Top drawdowns</span>
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

/** Colour a value by a quality heuristic (higher = better): green at/above
 * `good`, red at/below `bad`, neutral in between. */
function toneClass(v: number, good: number, bad: number): string {
  if (v >= good) return 'text-pos-400';
  if (v <= bad) return 'text-neg-400';
  return 'text-fg-soft';
}

/** One labelled metric in the strategy stat grid — small caption with an info
 * tooltip over a mono value. `tone` colours the value by quality; omit it for
 * purely informational metrics (tracking error, holdings, months). */
function Stat({ label, info, tone, children }: { label: string; info: ReactNode; tone?: string; children: ReactNode }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wide text-fg-faint flex items-center">
        {label}<CellInfoTip>{info}</CellInfoTip>
      </div>
      <div className={`font-mono text-sm ${tone ?? 'text-fg-soft'}`}>{children}</div>
    </div>
  );
}
