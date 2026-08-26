'use client';

import { useMemo } from 'react';
import { reverseDcfWorking, type SourceObs } from './egmInputs';
import { marketCapOf, type ReverseDcfInputs } from './reverseDcf';
import { type MetricRow } from './quickValuation';

/**
 * The three company figures the reverse DCF reads, with where each came from — the value, the
 * fiscal period and the metric code.
 *
 * ⚠ IT CALLS `reverseDcfWorking`, WHICH `reverseDcfSource` REDUCES TO THE PANEL'S SCALARS. One
 * extraction, two readers: the table cannot show a number the model was not given.
 */

export default function ReverseDcfInputsModal({
  metrics, currency, name, isin, fcf, target, discountRate, years, perpetuityGrowth, today, onClose,
}: {
  metrics: MetricRow[];
  /** ⚠ THE PANEL'S OWN `today`, so "next fiscal year" means the same period in both. */
  today: string;

  currency?: string | null;
  name?: string | null;
  isin: string;
  fcf: number | null;
  target: number | null;
  discountRate: number;
  years: number;
  perpetuityGrowth: number;
  onClose: () => void;
}) {
  const w = useMemo(() => reverseDcfWorking(metrics, today), [metrics, today]);
  const src = useMemo<ReverseDcfInputs>(() => ({
    price: w.price.used, sharesOutstanding: w.shares.used, fcf: w.fcf.used,
  }), [w]);

  const marketCap = marketCapOf(src);

  const ccy = currency ? `${currency} ` : '';
  const mn = (v: number | null | undefined) => (v == null ? 'n/a'
    : `${ccy}${Math.round(v).toLocaleString('en-US')}M`);
  const n2 = (v: number | null | undefined) => (v == null ? 'n/a' : v.toFixed(2));

  const ROWS: [string, SourceObs, 'money' | 'plain'][] = [
    ['Share price', w.price, 'plain'],
    ['Shares outstanding (m)', w.shares, 'plain'],
    ['Free cash flow', w.fcf, 'money'],
    // ⚠ THE ONE ROW WHOSE PERIOD IS IN THE FUTURE. Everything else in this table is a filing; this
    // is the consensus the forward base is derived from (`forwardFcf` nets capex off it), and its
    // `when` column is what makes the difference visible — the section's own caption says "as
    // filed, nothing forecast", which stopped being true the day the base could be a forecast.
    ['Operating cash flow (next FY, est.)', w.ocfEst, 'money'],
    ['WACC (%)', w.wacc, 'plain'],
  ];

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-full max-w-4xl h-[84vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">Reverse DCF — the data it reads</h2>
          {name && <span className="text-sm text-fg-soft truncate max-w-[28ch]" title={name}>{name}</span>}
          <span className="text-[12px] font-mono text-fg-faint">{isin}</span>
          <button type="button" onClick={onClose} className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-y-auto overflow-x-hidden px-6 py-4 space-y-6 min-w-0">
          <section className="space-y-2 min-w-0">
            <h3 className="text-sm font-semibold text-fg-strong">Company figures</h3>
            <p className="text-[12px] text-fg-faint break-words whitespace-normal max-w-[80ch]">
              The latest observation of each line, as filed and unadjusted — except the last
              estimate row, whose period is in the FUTURE and which is a consensus, not a filing.
            </p>
            <div className="overflow-auto rounded-lg border border-neutral-800/40 max-w-full">
              <table className="w-full text-xs">
                <thead className="bg-page">
                  <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
                    <th className="px-3 py-1.5 font-medium text-left">Input</th>
                    <th className="px-3 py-1.5 font-medium text-right">Value</th>
                    <th className="px-3 py-1.5 font-medium text-left">Period</th>
                    <th className="px-3 py-1.5 font-medium text-left">Metric code</th>
                  </tr>
                </thead>
                <tbody>
                  {ROWS.map(([label, obs, kind]) => (
                    <tr key={label} className="border-t border-neutral-800/40">
                      <td className="px-3 py-1 text-fg-soft whitespace-nowrap">{label}</td>
                      <td className={`px-3 py-1 text-right font-mono ${obs.used == null ? 'text-warn-300' : 'text-fg-soft'}`}>
                        {obs.used == null ? 'n/a' : (kind === 'money' ? mn(obs.used) : n2(obs.used))}
                      </td>
                      {/* ⚠⚠ A TTM ROW IS A SUM OF FOUR QUARTERS, NOT THE FIGURE FILED AT THAT DATE.
                          Printed bare, `2026-06-30 · −89,325` is a quarter-end over a number four
                          times any quarter's size — which reads as a vendor error rather than a
                          window, in the one table whose whole job is to show where a number came
                          from. See `egmInputs.ttmObs`. */}
                      <td className="px-3 py-1 text-fg-muted whitespace-nowrap">
                        {obs.date == null ? '—'
                          : obs.ttm ? `TTM to ${obs.date.slice(0, 10)}` : obs.date.slice(0, 10)}
                      </td>
                      <td className="px-3 py-1 font-mono text-[11px] text-fg-faint">{obs.code ?? '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="space-y-2 min-w-0">
            <h3 className="text-sm font-semibold text-fg-strong">What they add up to</h3>
            <dl className="text-[12px] space-y-1 max-w-[80ch]">
              <div className="flex gap-2 flex-wrap">
                <dt className="text-fg-muted w-56 shrink-0">Market cap</dt>
                <dd className="font-mono text-fg-soft break-words">
                  {n2(src.price)} × {src.sharesOutstanding == null ? 'n/a'
                    : Math.round(src.sharesOutstanding).toLocaleString('en-US')}M = {mn(marketCap)}
                </dd>
              </div>
              <div className="flex gap-2 flex-wrap">
                <dt className="text-fg-muted w-56 shrink-0">Cash flow compounded</dt>
                <dd className="font-mono text-fg-soft break-words">{mn(src.fcf)}</dd>
              </div>
              {/* Any assumption the reader has moved off its default is called out, so a figure in
                  the panel can be traced to a choice rather than to the data. */}
              {fcf != null && src.fcf != null && Math.abs(fcf - src.fcf) > 0.5 && (
                <div className="flex gap-2 flex-wrap">
                  <dt className="text-warn-300 w-56 shrink-0">Cash flow overridden to</dt>
                  <dd className="font-mono text-warn-300">{mn(fcf)}</dd>
                </div>
              )}
              {target != null && marketCap != null && Math.abs(target - marketCap) > 0.5 && (
                <div className="flex gap-2 flex-wrap">
                  <dt className="text-warn-300 w-56 shrink-0">Solving against</dt>
                  <dd className="font-mono text-warn-300">{mn(target)}</dd>
                </div>
              )}
              <div className="flex gap-2 flex-wrap">
                <dt className="text-fg-muted w-56 shrink-0">Model</dt>
                <dd className="font-mono text-fg-soft break-words">
                  {years}y at g, discounted {(discountRate * 100).toFixed(1)}%,
                  then {(perpetuityGrowth * 100).toFixed(0)}% in perpetuity
                </dd>
              </div>
            </dl>
          </section>
        </div>
      </div>
    </div>
  );
}
