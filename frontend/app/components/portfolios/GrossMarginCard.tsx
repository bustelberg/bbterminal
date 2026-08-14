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
import { Stat } from './MetricGrowthCard';
import { LegendItem } from './ChartLegend';
import { type Target } from './HoldingsRevenueModal';
import GrossMarginInputsModal from './GrossMarginInputsModal';
import { grossMarginByYear, type GrossMarginInputs } from './grossMarginData';
import { meanOf, paddedDomain , xToPeriod } from './marginData';
import { benchNote, benchmarkFirst, mergeSeries, useBenchInputs, withBench, type BenchTarget } from './benchSeries';

/**
 * Gross-margin card: Gross Profit ÷ Revenue per fiscal year, on a LINEAR % axis (a ratio, not a
 * compounding series — no log / exponential trend). What is left of each sale after the direct
 * cost of making it — the cleanest read on pricing power. Click through to the two base lines.
 *
 * ⚠ A BANK HAS NO GROSS MARGIN, AND THE CARD MUST SHOW A HOLE RATHER THAN A ZERO. GuruFocus's 'B'
 * industry template has no cost of goods sold, so the line is absent (JPMorgan) — the concept does
 * not apply. A 0 would draw a company selling at cost. Where too much of a book is like that the
 * year is dropped entirely (`MIN_YEAR_COVERAGE_PCT`), rather than publishing a "gross margin" that
 * silently describes only the half that has one.
 *
 * ⚠ DERIVED, THOUGH GURUFOCUS ALSO PUBLISHES `Ratios__Gross Margin %`. It reproduces their figure
 * exactly (ASML 2025: 17,258/32,667.3 = 52.83% vs published 52.83; Apple 46.91 vs 46.905) and
 * leaves two lines the drill-down can show — a published ratio has no workings to check.
 *
 * ⚠ THE RATIO IS DERIVED HERE from the raw lines (`grossMarginByYear`), so the line, the tiles and
 * the drill-down are one computation. Aggregation is a weight-weighted average of per-company
 * ratios — currency-safe, unlike summing mixed-currency amounts. Mirrors {@link ./SbcOcfCard}.
 */

export default function GrossMarginCard({ holdingsTarget, holdingsName, benchTarget }: {
  holdingsTarget: Target; holdingsName?: string | null;
  /** The index drawn beside the book — same endpoint, same helper. See `benchSeries`. */
  benchTarget?: BenchTarget | null;
}) {
  const [data, setData] = useState<GrossMarginInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/gross-margin-inputs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(holdingsTarget),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as GrossMarginInputs);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [holdingsTarget]);

  const marginByYr = useMemo(() => grossMarginByYear(data?.rows ?? []), [data]);

  const [benchData, benchErr] = useBenchInputs<GrossMarginInputs>('gross-margin-inputs', benchTarget);
  const benchByYr = useMemo(
    () => (benchData ? grossMarginByYear(benchData.rows) : null), [benchData]);

  const note = benchNote(benchTarget, benchData, benchErr, benchByYr);

  const chartData = useMemo(
    () => mergeSeries(marginByYr, benchByYr, 'margin'), [marginByYr, benchByYr]);

  const own = holdingsName ?? 'Gross margin';
  const avg = meanOf([...marginByYr.values()]);
  const latestYear = Math.max(-Infinity, ...marginByYr.keys());
  const latest = Number.isFinite(latestYear) ? marginByYr.get(latestYear) ?? null : null;
  const pct = (v: number | null) => (v == null ? '—' : `${v.toFixed(1)}%`);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <h4 className="text-base font-semibold text-fg-strong">Gross margin</h4>

      {data == null && !err ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : err ? (
        <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
      ) : marginByYr.size === 0 ? (
        <p className="text-[12px] text-fg-faint py-16 text-center">No gross-profit / revenue figures ingested to compute a margin.</p>
      ) : (
        <>
          <div className="flex flex-wrap gap-2">
            <Stat label="Avg" value={pct(avg)} color={chartTheme.accent}
              info={<InfoTip content={<AspectCard
                what="Average gross margin over the years shown — what is left of each sale after the direct cost of making it."
                where="Computed here — Gross Profit ÷ Revenue per year, weight-averaged across holdings. Reproduces GuruFocus's own `Gross Margin %` exactly, but leaves the two lines visible in the drill-down."
                when="The years on the chart."
                how="The share of each sales-euro reinvested in property, plant & intangibles. Lower = more capital-light (asset-heavy businesses read high)." />} />} />
            <Stat label="Latest" value={pct(latest)} color={chartTheme.accent} />
          </div>

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis dataKey="year" tickFormatter={xToPeriod} tick={{ fontSize: 12, fill: chartTheme.axisTick }} />
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
        <GrossMarginInputsModal target={holdingsTarget} portfolioName={holdingsName}
          benchTarget={benchTarget} benchLabel={benchTarget?.universe ?? null}
          onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
