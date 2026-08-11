'use client';

import { useMemo } from 'react';
import { type PerfRow, perfByPeriod } from './perfStats';
import { pct, ratio, ratioColor, retColor } from './format';

function Table({ title, rows }: { title: string; rows: PerfRow[] }) {
  return (
    <div className="space-y-1.5">
      <div className="text-xs font-semibold text-fg-strong uppercase tracking-wide">{title}</div>
      <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-80">
        <table className="w-full text-xs">
          <thead className="bg-card sticky top-0 text-fg-faint text-[11px] uppercase tracking-wide">
            <tr className="border-b border-neutral-800/40">
              <th className="px-3 py-1.5 text-left font-medium">Period</th>
              <th className="px-3 py-1.5 text-right font-medium" title="Annualized geometric return (CAGR)">Return</th>
              <th className="px-3 py-1.5 text-right font-medium" title="Annualized volatility of daily returns">Vol</th>
              <th className="px-3 py-1.5 text-right font-medium" title="Annualized arithmetic return ÷ volatility (rf = 0)">Sharpe</th>
              <th className="px-3 py-1.5 text-right font-medium" title="Annualized arithmetic return ÷ downside deviation (rf = 0)">Sortino</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
            {rows.map((r) => (
              <tr key={r.label} className="hover:bg-overlay/[0.02]">
                <td className="px-3 py-1.5 text-fg-soft whitespace-nowrap">{r.label}</td>
                <td className={`px-3 py-1.5 text-right font-mono ${retColor(r.ret)}`}>{pct(r.ret)}</td>
                <td className="px-3 py-1.5 text-right font-mono text-fg">{pct(r.vol)}</td>
                <td className={`px-3 py-1.5 text-right font-mono ${ratioColor(r.sharpe)}`}>{ratio(r.sharpe)}</td>
                <td className={`px-3 py-1.5 text-right font-mono ${ratioColor(r.sortino)}`}>{ratio(r.sortino)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/** Return / volatility / Sharpe / Sortino of the equal-weight index, bucketed by
 * calendar year, 5-year block, and 10-year block. Computed client-side from the
 * same daily index series that drives the chart. */
export default function PerfTables({ dates, level }: { dates: string[]; level: number[] }) {
  const byYear = useMemo(() => perfByPeriod(dates, level, 'year'), [dates, level]);
  const by5 = useMemo(() => perfByPeriod(dates, level, '5y'), [dates, level]);
  const by10 = useMemo(() => perfByPeriod(dates, level, '10y'), [dates, level]);
  if (!byYear.length) return null;

  return (
    <div className="space-y-3">
      <div className="grid gap-4 lg:grid-cols-3">
        <Table title="By year" rows={byYear} />
        <Table title="Every 5 years" rows={by5} />
        <Table title="Every 10 years" rows={by10} />
      </div>
      <p className="text-[11px] text-fg-faint leading-tight">
        Return = annualized geometric CAGR · Vol/Sharpe/Sortino annualized (252 trading days, risk-free = 0) ·
        Sharpe & Sortino use the annualized arithmetic-mean return · buckets with &lt; 20 trading days show “—” for the ratios.
      </p>
    </div>
  );
}
