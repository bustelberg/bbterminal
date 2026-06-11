'use client';

import { memo, useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  ReferenceLine, ReferenceArea, CartesianGrid,
} from 'recharts';
import InfoTip from '../InfoTip';
import Spinner from '../Spinner';
import { type ChartCadence, type MetricRow } from './types';
import { dropExtremeOutliers, tooltipStyle } from './utils';
import { buildMemberSeries, weightedAverageSeries, MemberRanking, type MemberSeries, type PortfolioMemberMetrics } from './portfolioBreakdown';
import { chartTheme } from '../../../lib/chartTheme';

// Series colors — must match the other earnings charts' A/B treatment so the
// user reads them consistently across panels.
const COLOR_A = chartTheme.accent; // primary series
const COLOR_B = chartTheme.compare;   // comparison series (violet — not a band colour)
const BAND_OPACITY = 0.18;

export type Band = 'good' | 'neutral' | 'poor';

/** Scoring rubric — mirrors SnapshotStats' `higherBetter`/`lowerBetter`.
 * `higher`: red below `poorBelow`, green at/above `goodAtOrAbove`, amber between.
 * `lower`:  green at/below `goodAtOrBelow`, red above `poorAbove`, amber between. */
export type BandSpec =
  | { kind: 'higher'; poorBelow: number; goodAtOrAbove: number }
  | { kind: 'lower'; goodAtOrBelow: number; poorAbove: number };

export function bandOf(v: number | null | undefined, spec: BandSpec): Band | null {
  if (v == null || !Number.isFinite(v)) return null;
  if (spec.kind === 'higher') {
    if (v < spec.poorBelow) return 'poor';
    if (v >= spec.goodAtOrAbove) return 'good';
    return 'neutral';
  }
  if (v <= spec.goodAtOrBelow) return 'good';
  if (v > spec.poorAbove) return 'poor';
  return 'neutral';
}

// Band → chart fill colour (status LED + latest-point marker).
const BAND_DOT: Record<Band, string> = {
  good: chartTheme.pos,
  neutral: chartTheme.warn,
  poor: chartTheme.neg,
};
// Band → text colour class — same scheme as SnapshotStats' SCORE_TEXT_COLOR
// so the value reads identically to the snapshot row.
const BAND_TEXT: Record<Band, string> = {
  good: 'text-pos-400',
  neutral: 'text-warn-300',
  poor: 'text-neg-400',
};

type Props = {
  metrics: MetricRow[];
  metricsB?: MetricRow[];
  labelA?: string;
  labelB?: string;
  /** Real company name shown in the hover tooltip (vs the short A/B pill tag).
   * Falls back to labelA/labelB/'A'/'B' when absent. */
  nameA?: string;
  nameB?: string;
  /** True during B's initial metrics fetch. Shows a spinner in the stat-pill
   * area for B so the comparison column doesn't just sit blank while loading. */
  loadingB?: boolean;
  /** Quarterly (default) or annual data. Threaded into buildSeries. */
  cadence?: ChartCadence;
  /** When true, drop impossible extreme outliers (off by default). */
  hideOutliers?: boolean;
  /** Builds the {date, value}[] series for one company's metrics at the
   * given cadence. */
  buildSeries: (m: MetricRow[], cadence: ChartCadence) => { date: string; value: number }[];
  /** Scoring bands (green/amber/red zones). */
  band: BandSpec;
  /** Formats a value for the header pills + tooltip (e.g. `15.00%`, `0.45`). */
  format: (v: number) => string;
  /** Formats a y-axis tick. Defaults to 2-decimal plain number. */
  axisFormat?: (v: number) => string;
  /** Grey subtitle line above the pills (e.g. "Total debt ÷ total equity"). */
  subtitle: string;
  /** Override the cadence word in the subtitle (e.g. 'daily'); defaults to the
   * active quarterly/annual cadence. */
  cadenceLabel?: string;
  /** InfoTip text beside the subtitle. */
  infoText: string;
  /** Shown when neither company has data. */
  emptyText: string;
  /** Per-member metrics when a side is a portfolio — drives the
   * ranked-by-impact holdings list in the tooltip. */
  breakdownA?: PortfolioMemberMetrics[];
  breakdownB?: PortfolioMemberMetrics[];
};

/** Custom tooltip used when a side is a portfolio: the blended value plus the
 * holdings ranked best→worst on this metric (recharts injects active/payload/
 * label). `dataKey` 'a'/'b' map to the two lines; `label` is the date. */
function BandBreakdownTooltip({
  active, payload, label,
  format, betterIsLower, nameA, nameB, memberSeriesA, memberSeriesB,
}: {
  active?: boolean;
  payload?: Array<{ dataKey?: string; value?: number }>;
  label?: string | number;
  format: (v: number) => string;
  betterIsLower: boolean;
  nameA: string;
  nameB: string;
  memberSeriesA: MemberSeries[] | null;
  memberSeriesB: MemberSeries[] | null;
}) {
  if (!active || !payload || payload.length === 0) return null;
  const date = String(label ?? '');
  const aVal = payload.find((p) => p.dataKey === 'a')?.value;
  const bVal = payload.find((p) => p.dataKey === 'b')?.value;
  return (
    <div style={tooltipStyle} className="text-xs min-w-[12rem]">
      <div className="text-fg-faint mb-1">{date.slice(0, 10)}</div>
      {aVal != null && (
        <div>
          <div className="flex items-center gap-3">
            <span style={{ color: COLOR_A }}>{nameA}</span>
            <span className="ml-auto font-mono text-fg-strong">{format(Number(aVal))}</span>
          </div>
          {memberSeriesA && <MemberRanking date={date} members={memberSeriesA} format={format} betterIsLower={betterIsLower} color={COLOR_A} />}
        </div>
      )}
      {bVal != null && (
        <div className="mt-2">
          <div className="flex items-center gap-3">
            <span style={{ color: COLOR_B }}>{nameB}</span>
            <span className="ml-auto font-mono text-fg-strong">{format(Number(bVal))}</span>
          </div>
          {memberSeriesB && <MemberRanking date={date} members={memberSeriesB} format={format} betterIsLower={betterIsLower} color={COLOR_B} />}
        </div>
      )}
    </div>
  );
}

/** Generic banded metric-over-time chart for the /earnings Charts grid. One
 * quarterly series per company, with green/amber/red background bands at the
 * SnapshotStats scoring thresholds, a status LED + band-coloured "Latest"
 * value in the header, a "Period avg" pill, and a band-coloured marker on the
 * latest point so the current zone reads at a glance. Powers every snapshot
 * stat that wanted "the Interest Coverage treatment". */
function MetricBandChartInner({
  metrics, metricsB, labelA, labelB, nameA, nameB, loadingB, cadence = 'quarterly', hideOutliers = false,
  buildSeries, band, format, axisFormat, subtitle, cadenceLabel, infoText, emptyText,
  breakdownA, breakdownB,
}: Props) {
  const buildOne = useMemo(
    () => (m: MetricRow[]) => {
      const s = buildSeries(m, cadence);
      return hideOutliers ? dropExtremeOutliers(s) : s;
    },
    [buildSeries, cadence, hideOutliers],
  );
  // Per-member series (portfolio sides only) — same builder as the line, so
  // each holding's value is directly comparable in the tooltip.
  const memberSeriesA = useMemo<MemberSeries[] | null>(
    () => (breakdownA ? buildMemberSeries(breakdownA, buildOne) : null),
    [breakdownA, buildOne],
  );
  const memberSeriesB = useMemo<MemberSeries[] | null>(
    () => (breakdownB ? buildMemberSeries(breakdownB, buildOne) : null),
    [breakdownB, buildOne],
  );

  // The line: for a portfolio, the weighted average of each holding's charted
  // value (correct even for derived series like PEG, where blending raw
  // components per date is meaningless). Single company → its own series.
  const seriesA = useMemo(
    () => (memberSeriesA ? weightedAverageSeries(memberSeriesA) : buildOne(metrics)),
    [memberSeriesA, buildOne, metrics],
  );
  const seriesB = useMemo(
    () => (memberSeriesB ? weightedAverageSeries(memberSeriesB) : (metricsB ? buildOne(metricsB) : [])),
    [memberSeriesB, buildOne, metricsB],
  );

  const meanA = useMemo(
    () => (seriesA.length > 0 ? seriesA.reduce((s, p) => s + p.value, 0) / seriesA.length : null),
    [seriesA],
  );
  const meanB = useMemo(
    () => (seriesB.length > 0 ? seriesB.reduce((s, p) => s + p.value, 0) / seriesB.length : null),
    [seriesB],
  );

  // Merge by date so Recharts can render both series on a single x-axis.
  const merged = useMemo(() => {
    const byDate = new Map<string, { date: string; a?: number; b?: number }>();
    for (const p of seriesA) byDate.set(p.date, { ...(byDate.get(p.date) ?? { date: p.date }), a: p.value });
    for (const p of seriesB) byDate.set(p.date, { ...(byDate.get(p.date) ?? { date: p.date }), b: p.value });
    return Array.from(byDate.values()).sort((x, y) => x.date.localeCompare(y.date));
  }, [seriesA, seriesB]);

  const hasB = !!metricsB;
  const hasNegative = merged.some((d) => (d.a != null && d.a < 0) || (d.b != null && d.b < 0));
  const latestA = seriesA.length > 0 ? seriesA[seriesA.length - 1].value : null;
  const latestB = seriesB.length > 0 ? seriesB[seriesB.length - 1].value : null;
  const latestDateA = seriesA.length > 0 ? seriesA[seriesA.length - 1].date : null;
  const latestDateB = seriesB.length > 0 ? seriesB[seriesB.length - 1].date : null;
  const bandA = bandOf(latestA, band);
  const bandB = bandOf(latestB, band);
  const aTag = hasB ? ` ${labelA ?? 'A'}` : '';
  const tick = axisFormat ?? ((v: number) => v.toFixed(2));

  // Y-domain: the union of (a) the data fit (min/max + ~8% margin) so the line
  // uses the vertical space, and (b) the threshold span plus a sliver past each
  // outer threshold — so a piece of BOTH the green and red zone is always
  // visible even when the data sits entirely inside one zone.
  const [yMin, yMax] = useMemo(() => {
    const t1 = band.kind === 'higher' ? band.poorBelow : band.goodAtOrBelow;
    const t2 = band.kind === 'higher' ? band.goodAtOrAbove : band.poorAbove;
    const lowThresh = Math.min(t1, t2);
    const highThresh = Math.max(t1, t2);
    // Sliver of the outer zones to reveal beyond each threshold.
    const zoneEps = (highThresh - lowThresh) * 0.5 || 1;
    const values = merged.flatMap((d) => [d.a, d.b].filter((v): v is number => v != null));
    if (values.length === 0) return [lowThresh - zoneEps, highThresh + zoneEps];
    const dataMin = Math.min(...values);
    const dataMax = Math.max(...values);
    const range = dataMax - dataMin;
    const margin = range > 0 ? range * 0.08 : Math.abs(dataMax) * 0.08 || 1;
    return [
      Math.min(dataMin - margin, lowThresh - zoneEps),
      Math.max(dataMax + margin, highThresh + zoneEps),
    ];
  }, [merged, band]);

  const fmtCell = (v: number | null) => (v == null ? '—' : format(v));

  if (seriesA.length === 0 && seriesB.length === 0) {
    return <div className="text-fg-subtle text-sm py-8 text-center">{emptyText}</div>;
  }

  return (
    <>
      <div className="text-fg-subtle text-xs mb-2 flex items-center gap-1 flex-wrap">
        {subtitle} · {cadenceLabel ?? (cadence === 'annual' ? 'annual' : 'quarterly')} <InfoTip text={infoText} />
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-1 mb-2">
        <div className="flex items-center gap-1">
          <div className="text-fg-subtle text-[11px]">Latest{aTag}</div>
          {bandA && <span className="inline-block w-2 h-2 rounded-full shrink-0" style={{ background: BAND_DOT[bandA] }} />}
          <div className={`font-mono text-xs ${bandA ? BAND_TEXT[bandA] : 'text-fg-strong'}`}>{fmtCell(latestA)}</div>
        </div>
        <div className="flex items-center gap-1">
          <div className="text-fg-subtle text-[11px]">Period avg{aTag}</div>
          <div className="font-mono text-xs" style={{ color: COLOR_A }}>{fmtCell(meanA)}</div>
        </div>
        {hasB && seriesB.length > 0 && (
          <>
            <div className="flex items-center gap-1">
              <div className="text-fg-subtle text-[11px]">Latest {labelB ?? 'B'}</div>
              {bandB && <span className="inline-block w-2 h-2 rounded-full shrink-0" style={{ background: BAND_DOT[bandB] }} />}
              <div className={`font-mono text-xs ${bandB ? BAND_TEXT[bandB] : 'text-fg-strong'}`}>{fmtCell(latestB)}</div>
            </div>
            <div className="flex items-center gap-1">
              <div className="text-fg-subtle text-[11px]">Period avg {labelB ?? 'B'}</div>
              <div className="font-mono text-xs" style={{ color: COLOR_B }}>{fmtCell(meanB)}</div>
            </div>
          </>
        )}
        {hasB && seriesB.length === 0 && loadingB && (
          <div className="flex items-center gap-1.5">
            <div className="text-fg-subtle text-[11px]">Loading {labelB ?? 'B'}</div>
            <Spinner size={10} />
          </div>
        )}
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={merged} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
          <XAxis dataKey="date" tick={{ fontSize: 10, fill: chartTheme.axisTick }} tickFormatter={(v: string) => v.slice(0, 7)} />
          <YAxis
            domain={[yMin, yMax]}
            allowDataOverflow
            tick={{ fontSize: 11, fill: chartTheme.axisTick }}
            tickFormatter={tick}
          />
          {/* Scoring bands (behind the lines) — same thresholds as the matching
              Snapshot Stats row. Orientation follows the rubric polarity. */}
          {band.kind === 'higher' ? (
            <>
              <ReferenceArea y1={yMin} y2={band.poorBelow} fill={chartTheme.neg} fillOpacity={BAND_OPACITY} ifOverflow="hidden" />
              <ReferenceArea y1={band.poorBelow} y2={band.goodAtOrAbove} fill={chartTheme.warn} fillOpacity={BAND_OPACITY} ifOverflow="hidden" />
              <ReferenceArea y1={band.goodAtOrAbove} y2={yMax} fill={chartTheme.pos} fillOpacity={BAND_OPACITY} ifOverflow="hidden" />
            </>
          ) : (
            <>
              <ReferenceArea y1={yMin} y2={band.goodAtOrBelow} fill={chartTheme.pos} fillOpacity={BAND_OPACITY} ifOverflow="hidden" />
              <ReferenceArea y1={band.goodAtOrBelow} y2={band.poorAbove} fill={chartTheme.warn} fillOpacity={BAND_OPACITY} ifOverflow="hidden" />
              <ReferenceArea y1={band.poorAbove} y2={yMax} fill={chartTheme.neg} fillOpacity={BAND_OPACITY} ifOverflow="hidden" />
            </>
          )}
          {hasNegative && <ReferenceLine y={0} stroke={chartTheme.axisTick} strokeDasharray="3 3" />}
          {memberSeriesA || memberSeriesB ? (
            <Tooltip content={
              <BandBreakdownTooltip
                format={format}
                betterIsLower={band.kind === 'lower'}
                nameA={nameA ?? labelA ?? 'A'}
                nameB={nameB ?? labelB ?? 'B'}
                memberSeriesA={memberSeriesA}
                memberSeriesB={memberSeriesB}
              />
            } />
          ) : (
            <Tooltip
              contentStyle={tooltipStyle}
              labelStyle={{ color: chartTheme.axisLabel }}
              formatter={(v, name) => {
                const lab = name === 'a' ? (nameA ?? labelA ?? 'A') : name === 'b' ? (nameB ?? labelB ?? 'B') : String(name);
                return [format(Number(v)), lab];
              }}
            />
          )}
          {/* Latest point gets an emphasized marker in its band colour, so the
              current zone (green/amber/red) shows on the plot, not just the pill. */}
          <Line
            type="monotone"
            dataKey="a"
            name="a"
            stroke={COLOR_A}
            strokeWidth={2}
            connectNulls
            dot={(props: { cx?: number; cy?: number; payload?: { date?: string } }) => {
              const { cx, cy, payload } = props;
              if (payload?.date === latestDateA && bandA) {
                return <circle cx={cx} cy={cy} r={4.5} fill={BAND_DOT[bandA]} stroke="#fff" strokeWidth={1.5} />;
              }
              return <circle cx={cx} cy={cy} r={0} fill="none" stroke="none" />;
            }}
          />
          {hasB && (
            <Line
              type="monotone"
              dataKey="b"
              name="b"
              stroke={COLOR_B}
              strokeWidth={2}
              connectNulls
              dot={(props: { cx?: number; cy?: number; payload?: { date?: string } }) => {
                const { cx, cy, payload } = props;
                if (payload?.date === latestDateB && bandB) {
                  return <circle cx={cx} cy={cy} r={4.5} fill={BAND_DOT[bandB]} stroke="#fff" strokeWidth={1.5} />;
                }
                return <circle cx={cx} cy={cy} r={0} fill="none" stroke="none" />;
              }}
            />
          )}
        </LineChart>
      </ResponsiveContainer>
    </>
  );
}

/** Memoized: the dashboard re-renders on every SSE log line during a data
 * refresh, but these props are referentially stable (memoized metrics, module-
 * const config, primitives), so the (recharts-heavy) chart only re-renders
 * when its data actually changes — not on unrelated dashboard state churn. */
const MetricBandChart = memo(MetricBandChartInner);
export default MetricBandChart;
