'use client';

import { useMemo } from 'react';

import type { BacktestResult } from '../../../lib/stores/momentum';
import { useFeeConfig } from '../../../lib/hooks/apiData';
import { computeFeeWaterfall } from './feeModel';

/**
 * Per-year fee waterfall for a backtest. Each row is one calendar year in
 * isolation: that year's gross return, then how much Leonteq took
 * (transaction + annual) with the return after, then how much Bustelberg
 * took (management + performance) with the net-to-client return. Leonteq is
 * always subtracted first, then Bustelberg. Reads the global fee config
 * (/fees) and applies the layered model client-side via `computeFeeWaterfall`.
 */
export default function FeeWaterfallPanel({ result }: { result: BacktestResult }) {
  const cfg = useFeeConfig();
  const w = useMemo(
    () =>
      computeFeeWaterfall(result.monthly_records, result.daily_records, cfg, {
        grossTotalReturnPct: result.summary?.total_return_pct,
      }),
    [result.monthly_records, result.daily_records, result.summary?.total_return_pct, cfg],
  );

  if (!w) return null;

  const signed = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40">
      <div className="px-5 py-3 border-b border-neutral-800/40 flex items-baseline justify-between">
        <h3 className="text-sm font-medium text-fg-strong">Fee waterfall</h3>
        <a href="/fees" className="text-xs text-accent-300 hover:text-accent-200">
          configure on /fees →
        </a>
      </div>
      <div className="px-5 py-4 space-y-3">
        <p className="text-[11px] text-fg-subtle leading-relaxed">
          Each row is one year <span className="text-fg-soft">in isolation</span>. Off that year&apos;s
          gross return, <span className="text-fg-soft">Leonteq&apos;s fees</span> come off first
          (transaction {cfg.transaction_bps}bps/trade + annual {cfg.leonteq_annual_bps}bps, final year
          pro-rated), then <span className="text-fg-soft">Bustelberg&apos;s</span> (management{' '}
          {cfg.bustelberg_mgmt_bps}bps + {cfg.bustelberg_perf_pct}% of gains above the running high-water
          mark). Reconciles exactly: gross − Leonteq − Bustelberg = net.
        </p>

        <div className="overflow-auto">
          <table className="w-full text-xs">
            <thead className="text-fg-subtle">
              <tr className="border-b border-neutral-800/40">
                <th className="text-left py-1.5 pr-3 font-medium">Year</th>
                <th className="text-right py-1.5 px-2 font-medium">Gross</th>
                <th className="text-right py-1.5 px-2 font-medium" title="What Leonteq took this year: per-trade transaction cost + annual fee">− Leonteq</th>
                <th className="text-right py-1.5 px-2 font-medium" title="Return after Leonteq's fees, before Bustelberg">= after Leonteq</th>
                <th className="text-right py-1.5 px-2 font-medium" title="What Bustelberg took this year: management fee + performance fee">− Bustelberg</th>
                <th className="text-right py-1.5 pl-2 font-medium">= Net to client</th>
              </tr>
            </thead>
            <tbody className="font-mono">
              {w.breakdown.map((b) => {
                const leonteqCut = b.transaction_pct + b.leonteq_annual_pct;
                const afterLeonteq = b.gross_return_pct - leonteqCut;
                const bustelbergCut = b.mgmt_pct + b.perf_pct;
                return (
                  <tr key={b.label} className="border-b border-neutral-800/20">
                    <td className="text-left py-1.5 pr-3 font-sans text-fg-soft">
                      {b.label}
                      {b.year_fraction < 0.99 && (
                        <span className="text-fg-faint"> ({(b.year_fraction * 100).toFixed(0)}%)</span>
                      )}
                    </td>
                    <td className={`text-right py-1.5 px-2 ${b.gross_return_pct >= 0 ? 'text-fg' : 'text-neg-300'}`}>
                      {signed(b.gross_return_pct)}
                    </td>
                    <td
                      className="text-right py-1.5 px-2 text-neg-300/80"
                      title={`transaction ${b.transaction_pct.toFixed(2)}% + annual ${b.leonteq_annual_pct.toFixed(2)}%`}
                    >
                      {leonteqCut === 0 ? '—' : `−${leonteqCut.toFixed(2)}%`}
                    </td>
                    <td className={`text-right py-1.5 px-2 ${afterLeonteq >= 0 ? 'text-fg-soft' : 'text-neg-300'}`}>
                      {signed(afterLeonteq)}
                    </td>
                    <td
                      className="text-right py-1.5 px-2 text-neg-300/80"
                      title={`management ${b.mgmt_pct.toFixed(2)}% + performance ${b.perf_pct.toFixed(2)}% (high-water mark ${b.hwm_pct.toFixed(2)}%)`}
                    >
                      {bustelbergCut === 0 ? '—' : `−${bustelbergCut.toFixed(2)}%`}
                    </td>
                    <td className={`text-right py-1.5 pl-2 font-medium ${b.net_return_pct >= 0 ? 'text-pos-300' : 'text-neg-300'}`}>
                      {signed(b.net_return_pct)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        <p className="text-[11px] text-fg-faint">
          Per-year returns compound to the cumulative totals — they don&apos;t add up linearly. Each
          year&apos;s &quot;Net to client&quot; is after all fees; the cumulative gross (net) is the
          parenthetical on each stat above.
        </p>
      </div>
    </div>
  );
}
