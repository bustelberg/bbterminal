'use client';

import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  ReferenceLine, ReferenceArea, CartesianGrid,
} from 'recharts';
import InfoTip from '../InfoTip';
import Spinner from '../Spinner';
import { type MetricRow } from './types';
import { interestCoverageQuarterlySeries, fmtNum, tooltipStyle } from './utils';
import { chartTheme } from '../../../lib/chartTheme';

// Series colors — must match ForwardPEChart / RelativeGrowthChart /
// FCFShareChart's A/B treatment so the user reads them consistently
// across panels.
const COLOR_A = chartTheme.accent; // primary series
const COLOR_B = chartTheme.warn;   // comparison series

// Scoring bands — mirror SnapshotStats' Interest Coverage rubric:
// `higherBetter(value, { poorBelow: 3, goodAtOrAbove: 7 })`. Below 3 is the
// covenant-warning red zone; 3–7 neutral/amber; ≥7 green.
const POOR_BELOW = 3;
const GOOD_AT_OR_ABOVE = 7;
const BAND_OPACITY = 0.08;

type Band = 'good' | 'neutral' | 'poor';

/** Which scoring band a coverage value falls in (null when not finite). */
function coverageBand(v: number | null | undefined): Band | null {
  if (v == null || !Number.isFinite(v)) return null;
  if (v < POOR_BELOW) return 'poor';
  if (v >= GOOD_AT_OR_ABOVE) return 'good';
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
  /** True during B's initial metrics fetch. Shows a spinner in the
   * stat-pill area for B so the comparison column doesn't just sit blank
   * while data is loading. */
  loadingB?: boolean;
};

/** Interest Coverage (Operating Income ÷ Interest Expense) over time —
 * how many times over operating earnings cover interest payments. Higher
 * is safer. Red/amber/green background bands mark the SnapshotStats
 * scoring zones (<3 / 3–7 / ≥7), and the latest point is emphasized with
 * a marker in its band colour (mirrored by a status LED + band-coloured
 * value in the header) so the current zone is readable at a glance. With
 * a comparison company the second series renders in amber. */
export default function InterestCoverageChart({ metrics, metricsB, labelA, labelB, loadingB }: Props) {
  const seriesA = useMemo(() => interestCoverageQuarterlySeries(metrics), [metrics]);
  const seriesB = useMemo(
    () => (metricsB ? interestCoverageQuarterlySeries(metricsB) : []),
    [metricsB],
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
  const bandA = coverageBand(latestA);
  const bandB = coverageBand(latestB);
  // Series tag is shown only in comparison mode (generic A/B, never the
  // company ticker). In single-company mode the pills read just "Latest".
  const aTag = hasB ? ` ${labelA ?? 'A'}` : '';

  // Y-domain pinned so the red/yellow/green scoring bands always render:
  // floor at 0 (or below, when coverage goes negative in an operating-loss
  // quarter) so the red zone is visible, and a ceiling that clears the green
  // threshold with headroom even when coverage never reaches it.
  const [yMin, yMax] = useMemo(() => {
    const values = merged.flatMap((d) => [d.a, d.b].filter((v): v is number => v != null));
    const dataMin = values.length ? Math.min(...values) : 0;
    const dataMax = values.length ? Math.max(...values) : GOOD_AT_OR_ABOVE;
    return [Math.min(0, dataMin * 1.05), Math.max(dataMax, GOOD_AT_OR_ABOVE) * 1.05];
  }, [merged]);

  if (seriesA.length === 0 && seriesB.length === 0) {
    return <div className="text-fg-subtle text-sm py-8 text-center">No interest coverage data. Refresh to load.</div>;
  }

  return (
    <>
      <div className="text-fg-subtle text-xs mb-2 flex items-center gap-1 flex-wrap">
        Operating income ÷ interest expense (×), per quarter <InfoTip text="Interest Coverage = Operating Income ÷ Interest Expense, computed per reporting quarter (matches the Snapshot Stats value). How many times over operating earnings cover interest payments. Higher is safer. Background bands: red below 3× (covenant-warning), amber 3–7×, green 7×+ — same thresholds as the Snapshot Stats row." />
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-1 mb-2">
        <div className="flex items-center gap-1">
          <div className="text-fg-subtle text-[11px]">Latest{aTag}</div>
          {bandA && <span className="inline-block w-2 h-2 rounded-full shrink-0" style={{ background: BAND_DOT[bandA] }} />}
          <div className={`font-mono text-xs ${bandA ? BAND_TEXT[bandA] : 'text-fg-strong'}`}>{latestA == null ? '—' : `${fmtNum(latestA, 2)}×`}</div>
        </div>
        <div className="flex items-center gap-1">
          <div className="text-fg-subtle text-[11px]">Period avg{aTag}</div>
          <div className="font-mono text-xs" style={{ color: COLOR_A }}>{meanA == null ? '—' : `${fmtNum(meanA, 2)}×`}</div>
        </div>
        {hasB && seriesB.length > 0 && (
          <>
            <div className="flex items-center gap-1">
              <div className="text-fg-subtle text-[11px]">Latest {labelB ?? 'B'}</div>
              {bandB && <span className="inline-block w-2 h-2 rounded-full shrink-0" style={{ background: BAND_DOT[bandB] }} />}
              <div className={`font-mono text-xs ${bandB ? BAND_TEXT[bandB] : 'text-fg-strong'}`}>{latestB == null ? '—' : `${fmtNum(latestB, 2)}×`}</div>
            </div>
            <div className="flex items-center gap-1">
              <div className="text-fg-subtle text-[11px]">Period avg {labelB ?? 'B'}</div>
              <div className="font-mono text-xs" style={{ color: COLOR_B }}>{meanB == null ? '—' : `${fmtNum(meanB, 2)}×`}</div>
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
            tickFormatter={(v: number) => v.toFixed(2)}
          />
          {/* Scoring bands (behind the lines): <3 red, 3–7 amber, ≥7 green —
              same thresholds as the Snapshot Stats Interest Coverage row. */}
          <ReferenceArea y1={yMin} y2={POOR_BELOW} fill={chartTheme.neg} fillOpacity={BAND_OPACITY} ifOverflow="hidden" />
          <ReferenceArea y1={POOR_BELOW} y2={GOOD_AT_OR_ABOVE} fill={chartTheme.warn} fillOpacity={BAND_OPACITY} ifOverflow="hidden" />
          <ReferenceArea y1={GOOD_AT_OR_ABOVE} y2={yMax} fill={chartTheme.pos} fillOpacity={BAND_OPACITY} ifOverflow="hidden" />
          {hasNegative && <ReferenceLine y={0} stroke={chartTheme.axisTick} strokeDasharray="3 3" />}
          <Tooltip
            contentStyle={tooltipStyle}
            labelStyle={{ color: chartTheme.axisLabel }}
            formatter={(v, name) => {
              const lab = name === 'a' ? (labelA ?? 'A') : name === 'b' ? (labelB ?? 'B') : String(name);
              return [`${Number(v).toFixed(2)}×`, lab];
            }}
          />
          {/* Latest point gets an emphasized marker filled with its band
              colour, so the current zone (green/amber/red) is visible right
              on the plot, not just in the header pill. */}
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
