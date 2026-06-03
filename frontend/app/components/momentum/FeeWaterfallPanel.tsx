'use client';

import { useMemo, useState } from 'react';

import type { BacktestResult } from '../../../lib/stores/momentum';
import { useFeeConfig } from '../../../lib/hooks/apiData';
import { computeFeeWaterfall } from './feeModel';

/** Signed, colored percentage. */
function Pct({ value, bold = false }: { value: number; bold?: boolean }) {
  const cls = value >= 0 ? 'text-emerald-400' : 'text-rose-400';
  return (
    <span className={`font-mono ${bold ? 'font-semibold' : ''} ${cls}`}>
      {value >= 0 ? '+' : ''}{value.toFixed(2)}%
    </span>
  );
}

/**
 * Per-backtest fee waterfall: Gross → after Leonteq → after Bustelberg
 * (net to client) → money accrued by Bustelberg. Reads the global fee
 * config (/fees) and applies the layered model client-side via
 * `computeFeeWaterfall`. An expandable "Calculation detail" shows the
 * per-year crystallization so the math is fully auditable.
 */
export default function FeeWaterfallPanel({ result }: { result: BacktestResult }) {
  const cfg = useFeeConfig();
  const [showDetail, setShowDetail] = useState(false);
  const w = useMemo(
    () =>
      computeFeeWaterfall(result.monthly_records, result.daily_records, cfg, {
        grossTotalReturnPct: result.summary?.total_return_pct,
      }),
    [result.monthly_records, result.daily_records, result.summary?.total_return_pct, cfg],
  );

  if (!w) return null;

  return (
    <div className="bg-[#151821] rounded-xl border border-gray-800/40">
      <div className="px-5 py-3 border-b border-gray-800/40 flex items-baseline justify-between">
        <h3 className="text-sm font-medium text-white">Fee waterfall</h3>
        <a href="/fees" className="text-xs text-indigo-300 hover:text-indigo-200">
          configure on /fees →
        </a>
      </div>
      <div className="px-5 py-4">
        <table className="w-full text-sm">
          <tbody className="divide-y divide-gray-800/30">
            <tr>
              <td className="py-2 text-gray-300">Gross return</td>
              <td className="py-2 text-right"><Pct value={w.gross_return_pct} /></td>
              <td className="py-2 pl-4 text-xs text-gray-600 w-1/2">strategy return, before any fees</td>
            </tr>
            <tr>
              <td className="py-2 text-gray-300">after Leonteq</td>
              <td className="py-2 text-right"><Pct value={w.after_leonteq_pct} /></td>
              <td className="py-2 pl-4 text-xs text-gray-500">
                <span className="text-rose-300/80">−{w.leonteq_drag_pp.toFixed(2)}pp</span>
                {' · '}txn {w.transaction_drag_pp.toFixed(2)}pp ({cfg.transaction_bps}bps/trade)
                {' + '}annual {w.leonteq_annual_drag_pp.toFixed(2)}pp ({cfg.leonteq_annual_bps}bps/yr)
              </td>
            </tr>
            <tr>
              <td className="py-2 text-white font-medium">
                after Bustelberg
                <span className="ml-2 text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-emerald-500/10 text-emerald-300 border-emerald-500/30">
                  net to client
                </span>
              </td>
              <td className="py-2 text-right"><Pct value={w.after_bustelberg_pct} bold /></td>
              <td className="py-2 pl-4 text-xs text-gray-500">
                <span className="text-rose-300/80">−{w.bustelberg_drag_pp.toFixed(2)}pp</span>
                {' · '}{cfg.bustelberg_mgmt_bps}bps/yr + {cfg.bustelberg_perf_pct}% HWM
              </td>
            </tr>
            <tr>
              <td className="py-2 text-amber-200">Bustelberg accrued</td>
              <td className="py-2 text-right font-mono text-amber-300">
                +{w.bustelberg_accrued_pct.toFixed(2)}%
              </td>
              <td className="py-2 pl-4 text-xs text-gray-500">
                mgmt {w.bustelberg_mgmt_pct.toFixed(2)}% · performance {w.bustelberg_perf_pct.toFixed(2)}%
                <span className="text-gray-600"> (of starting capital)</span>
              </td>
            </tr>
          </tbody>
        </table>

        <button
          type="button"
          onClick={() => setShowDetail((v) => !v)}
          className="mt-3 text-xs text-indigo-300 hover:text-indigo-200"
        >
          {showDetail ? '▾ Hide calculation detail' : '▸ Show calculation detail'}
        </button>

        {showDetail && (
          <div className="mt-3 bg-[#0f1117] border border-gray-800/40 rounded-lg p-3 space-y-3">
            <p className="text-[11px] text-gray-500 leading-relaxed">
              Each row is one year <span className="text-gray-300">in isolation</span>: that year&apos;s gross
              return, then what&apos;s deducted from it. Per-trade transaction cost ({cfg.transaction_bps}bps each
              buy/sell) and the Leonteq annual fee ({cfg.leonteq_annual_bps}bps, final year pro-rated) come off
              first; then Bustelberg&apos;s management fee ({cfg.bustelberg_mgmt_bps}bps), then the
              performance fee ({cfg.bustelberg_perf_pct}% of that year&apos;s gain above the running high-water
              mark — 0 in a year that doesn&apos;t set a new peak). Each row reconciles exactly:
              <span className="text-gray-400"> gross − transaction − annual − mgmt − perf = net</span>.
            </p>

            <div className="overflow-auto">
              <table className="w-full text-xs">
                <thead className="text-gray-500">
                  <tr className="border-b border-gray-800/40">
                    <th className="text-left py-1.5 pr-3 font-medium">Year</th>
                    <th className="text-right py-1.5 px-2 font-medium">Gross</th>
                    <th className="text-right py-1.5 px-2 font-medium" title="Per-trade transaction cost this year">− Txn</th>
                    <th className="text-right py-1.5 px-2 font-medium" title="Leonteq annual fee this year">− Annual</th>
                    <th className="text-right py-1.5 px-2 font-medium" title="Bustelberg management fee this year">− Mgmt</th>
                    <th className="text-right py-1.5 px-2 font-medium" title="Performance fee this year (above the high-water mark)">− Perf</th>
                    <th className="text-right py-1.5 px-2 font-medium" title="Running high-water-mark level (carries across years)">HWM</th>
                    <th className="text-right py-1.5 pl-2 font-medium">= Net</th>
                  </tr>
                </thead>
                <tbody className="font-mono">
                  {w.breakdown.map((b) => (
                    <tr key={b.label} className="border-b border-gray-800/20">
                      <td className="text-left py-1.5 pr-3 font-sans text-gray-300">
                        {b.label}
                        {b.year_fraction < 0.99 && (
                          <span className="text-gray-600"> ({(b.year_fraction * 100).toFixed(0)}%)</span>
                        )}
                      </td>
                      <td className={`text-right py-1.5 px-2 ${b.gross_return_pct >= 0 ? 'text-gray-200' : 'text-rose-300'}`}>
                        {b.gross_return_pct >= 0 ? '+' : ''}{b.gross_return_pct.toFixed(2)}%
                      </td>
                      <td className="text-right py-1.5 px-2 text-rose-300/80">{b.transaction_pct === 0 ? '—' : `−${b.transaction_pct.toFixed(2)}%`}</td>
                      <td className="text-right py-1.5 px-2 text-rose-300/80">{b.leonteq_annual_pct === 0 ? '—' : `−${b.leonteq_annual_pct.toFixed(2)}%`}</td>
                      <td className="text-right py-1.5 px-2 text-rose-300/80">{b.mgmt_pct === 0 ? '—' : `−${b.mgmt_pct.toFixed(2)}%`}</td>
                      <td className="text-right py-1.5 px-2 text-rose-300/80">{b.perf_pct === 0 ? '—' : `−${b.perf_pct.toFixed(2)}%`}</td>
                      <td className="text-right py-1.5 px-2 text-gray-600">{b.hwm_pct.toFixed(2)}%</td>
                      <td className={`text-right py-1.5 pl-2 font-medium ${b.net_return_pct >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                        {b.net_return_pct >= 0 ? '+' : ''}{b.net_return_pct.toFixed(2)}%
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <p className="text-[11px] text-gray-600">
              Per-year returns compound to the cumulative totals shown above — they don&apos;t add up
              linearly. &quot;Net&quot; here is each year&apos;s after-all-fees return; the cumulative net
              is the <span className="text-gray-400">(net)</span> parenthetical on each stat.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
