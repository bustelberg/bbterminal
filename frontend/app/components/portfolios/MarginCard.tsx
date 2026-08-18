'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  CartesianGrid, ComposedChart, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { useLang } from '../../../lib/i18n';
import { chartTitle } from './longEquityCopy';
import { Stat } from './MetricGrowthCard';
import { LegendItem } from './ChartLegend';
import { type Target } from './HoldingsRevenueModal';
import { fcfLabel } from './sbcCorrection';
import MarginInputsModal from './MarginInputsModal';
import { marginByYear, meanOf, paddedDomain, type MarginInputs , xToPeriod } from './marginData';
import { periodAxis } from '../../../lib/chartAxis';
import { benchNote, benchmarkFirst, mergeSeries, useBenchInputs, withBench, type BenchTarget } from './benchSeries';

/**
 * FCF-SBC margin card: (Free Cash Flow − Stock-Based Compensation) ÷ Revenue per fiscal year, on a
 * LINEAR % axis (it's a ratio, not a compounding series — so no log / exponential trend). The
 * benchmark line is the index's same margin, directly comparable (both %). Click through to the
 * three base inputs per company.
 *
 * ⚠ THE MARGIN IS DERIVED HERE from the raw lines (`marginByYear`), so the line, the tiles and the
 * drill-down are the same computation. Aggregation is a weight-weighted average of per-company
 * margins — currency-safe, unlike summing mixed-currency amounts.
 */

export default function MarginCard({ holdingsTarget, holdingsName, sbcCorrection = true, benchTarget }: {
  holdingsTarget: Target; holdingsName?: string | null;
  /** Tab-level toggle. ⚠ This card USED to subtract SBC unconditionally; it now follows
   *  the checkbox, and its title changes with it. */
  sbcCorrection?: boolean;
  /** The index to draw beside the book — same endpoint, same helper. See `benchSeries`. */
  benchTarget?: BenchTarget | null;
}) {
  // ⚠ READ FROM THE STORE, NOT DRILLED THROUGH `LongEquityTab` AS A PROP — see the same
  // note on the sibling cards. `useLang` is an external store (`lib/i18n.ts`).
  const [lang] = useLang();
  const [data, setData] = useState<MarginInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/margin-inputs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(holdingsTarget),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as MarginInputs);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [holdingsTarget]);

  const marginByYr = useMemo(
    () => marginByYear(data?.rows ?? [], sbcCorrection), [data, sbcCorrection]);

  const [benchData, benchErr] = useBenchInputs<MarginInputs>('margin-inputs', benchTarget);
  const benchByYr = useMemo(
    () => (benchData ? marginByYear(benchData.rows, sbcCorrection) : null), [benchData, sbcCorrection]);

  const note = benchNote(benchTarget, benchData, benchErr, benchByYr);

  const chartData = useMemo(
    () => mergeSeries(marginByYr, benchByYr, 'margin'), [marginByYr, benchByYr]);

  const own = holdingsName ?? 'Margin';
  const avg = meanOf([...marginByYr.values()]);
  const latestYear = Math.max(-Infinity, ...marginByYr.keys());
  const latest = Number.isFinite(latestYear) ? marginByYr.get(latestYear) ?? null : null;
  const pct = (v: number | null) => (v == null ? '—' : `${v.toFixed(1)}%`);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <h4 className="text-base font-semibold text-fg-strong">{chartTitle(lang, 'fcfMargin', sbcCorrection)}</h4>

      {data == null && !err ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : err ? (
        <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
      ) : marginByYr.size === 0 ? (
        <p className="text-[12px] text-fg-faint py-16 text-center">No revenue / FCF ingested to compute a margin.</p>
      ) : (
        <>
          <div className="flex flex-wrap gap-2">
            <Stat label="Avg margin" value={pct(avg)} color={chartTheme.accent}
              info={<InfoTip content={<AspectCard
                what={`Average ${fcfLabel(sbcCorrection)} margin over the years shown.`}
                where="Computed here — (FCF − SBC) ÷ Revenue per year, weight-averaged across holdings."
                when="The years on the chart." how="SBC is a non-cash add-back to FCF, so subtracting it gives a truer cash margin." />} />} />
            <Stat label="Latest" value={pct(latest)} color={chartTheme.accent} />
          </div>

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis {...periodAxis(xToPeriod)} />
                <YAxis domain={paddedDomain(withBench(marginByYr.values(), benchByYr))} tick={{ fontSize: 12, fill: chartTheme.axisTick }} width={48}
                  tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }} itemSorter={benchmarkFirst}
                  formatter={(v, n) => [`${typeof v === 'number' ? v.toFixed(1) : '—'}%`, n === 'bench' ? (benchTarget?.universe ?? 'Benchmark') : own]} />
                <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
                {avg != null && <ReferenceLine y={avg} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />}
                <Line dataKey="margin" name="margin" type="monotone" stroke={chartTheme.accent} strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                {benchByYr && <Line dataKey="bench" name="bench" type="monotone" stroke={chartTheme.pos} strokeWidth={2} dot={{ r: 2 }} connectNulls />}
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <LegendItem color={chartTheme.accent} label={own} />
              {avg != null && <LegendItem color={chartTheme.accent} stroke="dashed"
                label={`${own} average`} />}
              {benchByYr && <LegendItem color={chartTheme.pos} label={benchTarget?.universe} />}
              {note && (
                <span className="text-fg-faint" title="An overlay that simply does not appear is indistinguishable from an index that matches this book exactly. Full detail is in the console.">
                  {note}
                </span>
              )}
            </div>
          </div>
        </>
      )}

      {showInputs && (
        <MarginInputsModal target={holdingsTarget} portfolioName={holdingsName}
          benchTarget={benchTarget} benchLabel={benchTarget?.universe ?? null}
          onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
