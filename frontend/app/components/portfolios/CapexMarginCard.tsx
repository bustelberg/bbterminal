'use client';

import { savedBenchmarkSeries } from './benchmarkSnapshot';

import { useMemo, useState } from 'react';
import {
  CartesianGrid, ComposedChart, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { pairedSpan, RatioStats } from './CardStats';
import { withWorked, workedMean } from './workedFormula';
import { LegendItem } from './ChartLegend';
import { type Target } from './HoldingsRevenueModal';
import MissingFundamentals from './MissingFundamentals';
import { useChartHoldingsInputs } from './CompanyChartInputs';
import CapexMarginInputsModal from './CapexMarginInputsModal';
import { capexMarginByYear, type CapexMarginInputs } from './capexMarginData';
import { paddedDomain , xToPeriod } from './marginData';
import { periodAxis } from '../../../lib/chartAxis';
import { benchNote, benchmarkFirst, mergeSeries, useBenchInputs, withBench, type BenchTarget } from './benchSeries';
import CardHeading from './CardHeading';

/**
 * Capex-margin card: |Capex| ÷ Revenue per fiscal year, on a LINEAR % axis (a ratio, not a
 * compounding series — no log / exponential trend). Capital intensity: the share of sales
 * reinvested in capex. Lower = more capital-light. Click through to the two base lines per company.
 *
 *  The ratio is derived here from the raw lines (`capexMarginByYear`), so the line, the tiles and
 * the drill-down are one computation. Aggregation is a weight-weighted average of per-company
 * ratios — currency-safe, unlike summing mixed-currency amounts. Mirrors {@link ./SbcOcfCard}.
 */

/**  `String.raw`, or every backslash in the expressions below is eaten before KaTeX
 *  sees it. */
const R = String.raw;

export default function CapexMarginCard({ holdingsTarget, holdingsName, benchTarget }: {
  holdingsTarget: Target; holdingsName?: string | null;
  /** The index drawn beside the book — same endpoint, same helper. See `benchSeries`. */
  benchTarget?: BenchTarget | null;
}) {
  const [showInputs, setShowInputs] = useState(false);

  const { data, error: err } = useChartHoldingsInputs<CapexMarginInputs>('capex-margin-inputs', holdingsTarget);

  const marginByYr = useMemo(() => capexMarginByYear(data?.rows ?? []), [data]);

  const [benchData, benchErr] = useBenchInputs<CapexMarginInputs>('capex-margin-inputs', benchTarget, undefined, true);
  const benchByYr = useMemo(
    () => (benchData ? savedBenchmarkSeries(benchData, 'default', () => capexMarginByYear(benchData.rows)) : null), [benchData]);

  const note = benchNote(benchTarget, benchData, benchErr, benchByYr);

  const chartData = useMemo(
    () => mergeSeries(marginByYr, benchByYr, 'margin'), [marginByYr, benchByYr]);

  const own = holdingsName ?? 'Capex margin';
  /**
   * The book's figures and the benchmark's, over the ONE window both lines cover — see
   * `CardStats`/`sharedSpan`.  COMPUTED ONCE: `own.avg` is BOTH the tile and the dashed average
   * line on the chart below, so the card cannot plot a mean it does not print.
   */
  const stats = useMemo(() => pairedSpan(marginByYr, benchByYr), [marginByYr, benchByYr]);
  const avg = stats.own.avg;
  const pct = (v: number | null) => (v == null ? '—' : `${v.toFixed(1)}%`);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <CardHeading chartKey="capexMargin" />

      {(data == null || (!!benchTarget && benchData == null)) && !(err || benchErr) ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : (err || benchErr) ? (
        <p className="text-xs text-neg-300 py-16 text-center">{err || benchErr}</p>
      ) : marginByYr.size === 0 ? (
          <MissingFundamentals message="No capex / revenue figures ingested to compute a margin." />
      ) : (
        <>
          <RatioStats stats={stats} benchLabel={benchTarget?.label} fmt={pct}
            avgInfo={<InfoTip content={<AspectCard
              what="Average capex margin (capital intensity) over the years shown."
              where="Computed here, per year, then weight-averaged across holdings."
              when="The years on the chart."
              worked={withWorked(
                R`\text{capex margin} = \dfrac{|\text{capex}|}{\text{revenue}}`,
                workedMean(stats.own.values))}
              how="The share of each sales-euro reinvested in property, plant & intangibles. Lower = more capital-light (asset-heavy businesses read high)." />} />} />

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis {...periodAxis(xToPeriod)} />
                <YAxis domain={paddedDomain(withBench(marginByYr.values(), benchByYr))} tick={{ fontSize: 12, fill: chartTheme.axisTick }} width={48}
                  tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }} itemSorter={benchmarkFirst}
                  formatter={(v, n) => [`${typeof v === 'number' ? v.toFixed(1) : '—'}%`, n === 'bench' ? (benchTarget?.label ?? 'Benchmark') : own]} />
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
              {benchByYr && <LegendItem color={chartTheme.pos} label={benchTarget?.label} />}
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
        <CapexMarginInputsModal target={holdingsTarget} portfolioName={holdingsName}
          benchTarget={benchTarget} benchLabel={benchTarget?.label ?? null}
          onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
