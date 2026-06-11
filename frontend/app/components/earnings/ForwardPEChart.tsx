'use client';

import { memo, useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid,
} from 'recharts';
import InfoTip from '../InfoTip';
import Spinner from '../Spinner';
import { MC, type MetricRow } from './types';
import { timeSeries, dropExtremeOutliers, tooltipStyle } from './utils';
import { buildMemberSeries, weightedAverageSeries, MemberRanking, type MemberSeries, type PortfolioMemberMetrics } from './portfolioBreakdown';
import { chartTheme } from '../../../lib/chartTheme';

const FMT_PE = (v: number) => `${v.toFixed(1)}x`;

// Series colors — A matches the dashboard's primary accent, B the
// consistent "comparison" hue across all earnings charts.
const COLOR_A = chartTheme.accent; // primary series
const COLOR_B = chartTheme.compare;   // comparison series (violet — not a band colour)

/** Tooltip for portfolio Forward P/E: blended value + holdings ranked
 * cheapest→richest (lower P/E is "better impact"). recharts injects
 * active/payload/label. */
function PEBreakdownTooltip({
  active, payload, label, nameA, nameB, memberSeriesA, memberSeriesB,
}: {
  active?: boolean;
  payload?: Array<{ dataKey?: string; value?: number }>;
  label?: string | number;
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
            <span className="ml-auto font-mono text-fg-strong">{FMT_PE(Number(aVal))}</span>
          </div>
          {memberSeriesA && <MemberRanking date={date} members={memberSeriesA} format={FMT_PE} betterIsLower color={COLOR_A} />}
        </div>
      )}
      {bVal != null && (
        <div className="mt-2">
          <div className="flex items-center gap-3">
            <span style={{ color: COLOR_B }}>{nameB}</span>
            <span className="ml-auto font-mono text-fg-strong">{FMT_PE(Number(bVal))}</span>
          </div>
          {memberSeriesB && <MemberRanking date={date} members={memberSeriesB} format={FMT_PE} betterIsLower color={COLOR_B} />}
        </div>
      )}
    </div>
  );
}

/** Single-series line chart of Forward P/E over time with red dashed
 * reference line at the period average. When `metricsB` is supplied, a
 * second line is overlaid in amber alongside its own period-average
 * value in the header. Hides itself when there's no Forward P/E data
 * for either company. */
function ForwardPEChartInner({
  metrics,
  metricsB,
  labelA,
  labelB,
  nameA,
  nameB,
  hideOutliers = false,
  loadingB,
  breakdownA,
  breakdownB,
}: {
  metrics: MetricRow[];
  metricsB?: MetricRow[];
  labelA?: string;
  labelB?: string;
  /** Real company name shown in the hover tooltip (vs the short A/B pill tag). */
  nameA?: string;
  nameB?: string;
  /** When true, drop impossible extreme outliers (off by default). */
  hideOutliers?: boolean;
  /** True during B's initial metrics fetch — shows a spinner where
   * B's "Current" / "Period avg" pills will appear, instead of
   * silently rendering only A's line. */
  loadingB?: boolean;
  /** Per-member metrics when a side is a portfolio — ranked holdings in tooltip. */
  breakdownA?: PortfolioMemberMetrics[];
  breakdownB?: PortfolioMemberMetrics[];
}) {
  const buildOne = useMemo(
    () => (m: MetricRow[]) => {
      const d = timeSeries(m, MC.FWD_PE);
      return hideOutliers ? dropExtremeOutliers(d) : d;
    },
    [hideOutliers],
  );
  const memberSeriesA = useMemo<MemberSeries[] | null>(
    () => (breakdownA ? buildMemberSeries(breakdownA, buildOne) : null),
    [breakdownA, buildOne],
  );
  const memberSeriesB = useMemo<MemberSeries[] | null>(
    () => (breakdownB ? buildMemberSeries(breakdownB, buildOne) : null),
    [breakdownB, buildOne],
  );
  // Portfolio line = weighted average of each holding's Forward P/E.
  const dataA = useMemo(
    () => (memberSeriesA ? weightedAverageSeries(memberSeriesA) : buildOne(metrics)),
    [memberSeriesA, buildOne, metrics],
  );
  const dataB = useMemo(
    () => (memberSeriesB ? weightedAverageSeries(memberSeriesB) : (metricsB ? buildOne(metricsB) : [])),
    [memberSeriesB, buildOne, metricsB],
  );

  const hasB = !!metricsB;

  // When comparing two companies whose series cover different date ranges,
  // averaging each over its own full range makes the "Period avg" / "Current"
  // pills incomparable (they'd measure different windows). In comparison mode
  // we therefore restrict the headline stats to the OVERLAPPING window common
  // to both series — the lines themselves still show each company's full
  // history. Single-company mode (and the no-overlap fallback) keep the full
  // series. `dataA`/`dataB` are sorted ascending by date (see `timeSeries`).
  const overlap = useMemo(() => {
    if (!hasB || dataA.length === 0 || dataB.length === 0) return null;
    const start = dataA[0].date > dataB[0].date ? dataA[0].date : dataB[0].date;
    const endA = dataA[dataA.length - 1].date;
    const endB = dataB[dataB.length - 1].date;
    const end = endA < endB ? endA : endB;
    return start <= end ? { start, end } : null;
  }, [hasB, dataA, dataB]);

  // Stats slices: the overlap window in comparison mode, else the full series.
  const statsA = useMemo(
    () => (overlap ? dataA.filter((p) => p.date >= overlap.start && p.date <= overlap.end) : dataA),
    [dataA, overlap],
  );
  const statsB = useMemo(
    () => (overlap ? dataB.filter((p) => p.date >= overlap.start && p.date <= overlap.end) : dataB),
    [dataB, overlap],
  );

  const meanA = useMemo(() => {
    if (statsA.length === 0) return 0;
    return statsA.reduce((s, d) => s + d.value, 0) / statsA.length;
  }, [statsA]);
  const meanB = useMemo(() => {
    if (statsB.length === 0) return 0;
    return statsB.reduce((s, d) => s + d.value, 0) / statsB.length;
  }, [statsB]);

  // Recharts wants one row per x-coordinate with all series flattened
  // into named keys. Merge A and B by date so both lines share the same
  // axis — gaps are connectNulls'd at the Line level. (Full series — the
  // overlap restriction only applies to the headline stats above.)
  const merged = useMemo(() => {
    const byDate = new Map<string, { date: string; a?: number; b?: number }>();
    for (const p of dataA) byDate.set(p.date, { ...(byDate.get(p.date) ?? { date: p.date }), a: p.value });
    for (const p of dataB) byDate.set(p.date, { ...(byDate.get(p.date) ?? { date: p.date }), b: p.value });
    return Array.from(byDate.values()).sort((x, y) => x.date.localeCompare(y.date));
  }, [dataA, dataB]);

  // "Current" = latest point WITHIN the stats window so A and B are quoted at
  // comparable dates (the common end), not each series' own latest.
  const latestA = statsA.length > 0 ? statsA[statsA.length - 1] : null;
  const latestB = statsB.length > 0 ? statsB[statsB.length - 1] : null;
  // Series tag is shown only in comparison mode (generic A/B, never the
  // company ticker). In single-company mode the pills read just "Current".
  const aTag = hasB ? ` ${labelA ?? 'A'}` : '';

  if (dataA.length === 0 && dataB.length === 0) {
    return <div className="text-fg-subtle text-sm py-8 text-center">No Forward P/E data. Refresh to load.</div>;
  }

  return (
    <>
      <div className="text-fg-subtle text-xs mb-2 flex items-center gap-2 flex-wrap">
        {latestA && (
          <>
            <span>Current{aTag}:</span>
            <span className="font-mono" style={{ color: COLOR_A }}>{latestA.value.toFixed(1)}x</span>
            <span className="text-fg-faint font-mono">({latestA.date})</span>
          </>
        )}
        {hasB && latestB && (
          <>
            <span className="text-fg-dim">·</span>
            <span>Current {labelB ?? 'B'}:</span>
            <span className="font-mono" style={{ color: COLOR_B }}>{latestB.value.toFixed(1)}x</span>
            <span className="text-fg-faint font-mono">({latestB.date})</span>
          </>
        )}
        {hasB && !latestB && loadingB && (
          <>
            <span className="text-fg-dim">·</span>
            <span>Current {labelB ?? 'B'}:</span>
            <Spinner size={10} />
          </>
        )}
        <span className="text-fg-dim">·</span>
        <span>Period avg{aTag}:</span>
        <span className="font-mono" style={{ color: COLOR_A }}>{meanA.toFixed(1)}x</span>
        {/* Period avg B is shown in-line only when comparison is active. */}
        {hasB && dataB.length > 0 && (
          <>
            <span className="text-fg-dim">·</span>
            <span>Period avg {labelB ?? 'B'}:</span>
            <span className="font-mono" style={{ color: COLOR_B }}>{meanB.toFixed(1)}x</span>
          </>
        )}
        {hasB && dataB.length === 0 && loadingB && (
          <>
            <span className="text-fg-dim">·</span>
            <span>Period avg {labelB ?? 'B'}:</span>
            <Spinner size={10} />
          </>
        )}
        {/* In comparison mode, surface the window the stats are measured over
            so the user knows A and B are quoted apples-to-apples. */}
        {hasB && overlap && (
          <>
            <span className="text-fg-dim">·</span>
            <span className="text-fg-faint">
              common period {overlap.start.slice(0, 7)} – {overlap.end.slice(0, 7)}
            </span>
          </>
        )}
        {hasB && !overlap && dataA.length > 0 && dataB.length > 0 && (
          <>
            <span className="text-fg-dim">·</span>
            <span className="text-warn-400">date ranges don’t overlap — stats cover each company’s own period</span>
          </>
        )}
        <InfoTip text="Forward P/E = Price / Next-year EPS estimate. Lower = cheaper relative to expected earnings. 'Period avg' is the average Forward P/E across the period. When comparing two companies, 'Current' and 'Period avg' are measured over the date range common to both series, so the values stay directly comparable even if one company has a longer history (the lines still show each company's full history)." />
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={merged} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
          <XAxis dataKey="date" tick={{ fontSize: 10, fill: chartTheme.axisTick }} tickFormatter={(v: string) => v.slice(0, 7)} />
          <YAxis tick={{ fontSize: 11, fill: chartTheme.axisTick }} tickFormatter={(v: number) => `${v.toFixed(0)}x`} />
          {memberSeriesA || memberSeriesB ? (
            <Tooltip content={
              <PEBreakdownTooltip
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
                return [`${Number(v).toFixed(1)}x`, lab];
              }}
            />
          )}
          <Line type="monotone" dataKey="a" name="a" stroke={COLOR_A} strokeWidth={2} dot={false} connectNulls />
          {hasB && <Line type="monotone" dataKey="b" name="b" stroke={COLOR_B} strokeWidth={2} dot={false} connectNulls />}
        </LineChart>
      </ResponsiveContainer>
    </>
  );
}

// Memoized — see MetricBandChart: avoids re-rendering on SSE-log churn during refresh.
const ForwardPEChart = memo(ForwardPEChartInner);
export default ForwardPEChart;
