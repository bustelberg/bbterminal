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
import { chartTheme } from '../../../lib/chartTheme';

// Series colors — A matches the dashboard's primary accent, B the
// consistent "comparison" hue across all earnings charts.
const COLOR_A = chartTheme.accent; // primary series
const COLOR_B = chartTheme.compare;   // comparison series (violet — not a band colour)

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
}) {
  const dataA = useMemo(() => {
    const d = timeSeries(metrics, MC.FWD_PE);
    return hideOutliers ? dropExtremeOutliers(d) : d;
  }, [metrics, hideOutliers]);
  const dataB = useMemo(() => {
    if (!metricsB) return [];
    const d = timeSeries(metricsB, MC.FWD_PE);
    return hideOutliers ? dropExtremeOutliers(d) : d;
  }, [metricsB, hideOutliers]);

  const meanA = useMemo(() => {
    if (dataA.length === 0) return 0;
    return dataA.reduce((s, d) => s + d.value, 0) / dataA.length;
  }, [dataA]);
  const meanB = useMemo(() => {
    if (dataB.length === 0) return 0;
    return dataB.reduce((s, d) => s + d.value, 0) / dataB.length;
  }, [dataB]);

  // Recharts wants one row per x-coordinate with all series flattened
  // into named keys. Merge A and B by date so both lines share the same
  // axis — gaps are connectNulls'd at the Line level.
  const merged = useMemo(() => {
    const byDate = new Map<string, { date: string; a?: number; b?: number }>();
    for (const p of dataA) byDate.set(p.date, { ...(byDate.get(p.date) ?? { date: p.date }), a: p.value });
    for (const p of dataB) byDate.set(p.date, { ...(byDate.get(p.date) ?? { date: p.date }), b: p.value });
    return Array.from(byDate.values()).sort((x, y) => x.date.localeCompare(y.date));
  }, [dataA, dataB]);

  const latestA = dataA.length > 0 ? dataA[dataA.length - 1] : null;
  const latestB = dataB.length > 0 ? dataB[dataB.length - 1] : null;
  const hasB = !!metricsB;
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
        <InfoTip text="Forward P/E = Price / Next-year EPS estimate. Lower = cheaper relative to expected earnings. 'Period avg' is the average across the visible period for the primary company — useful for spotting when the stock trades above or below its typical valuation." />
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={merged} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
          <XAxis dataKey="date" tick={{ fontSize: 10, fill: chartTheme.axisTick }} tickFormatter={(v: string) => v.slice(0, 7)} />
          <YAxis tick={{ fontSize: 11, fill: chartTheme.axisTick }} tickFormatter={(v: number) => `${v.toFixed(0)}x`} />
          <Tooltip
            contentStyle={tooltipStyle}
            labelStyle={{ color: chartTheme.axisLabel }}
            formatter={(v, name) => {
              const lab = name === 'a' ? (nameA ?? labelA ?? 'A') : name === 'b' ? (nameB ?? labelB ?? 'B') : String(name);
              return [`${Number(v).toFixed(1)}x`, lab];
            }}
          />
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
