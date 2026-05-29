'use client';

import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  ReferenceLine, CartesianGrid,
} from 'recharts';
import InfoTip from '../InfoTip';
import Spinner from '../Spinner';
import { MC, type MetricRow } from './types';
import { timeSeries, tooltipStyle } from './utils';

// Series colors — A indigo matches the rest of the dashboard's primary
// accent, B amber is the consistent "comparison" hue across all
// earnings charts.
const COLOR_A = '#818cf8'; // indigo-400
const COLOR_B = '#f59e0b'; // amber-500

/** Single-series line chart of Forward P/E over time with red dashed
 * reference line at the period average. When `metricsB` is supplied, a
 * second line is overlaid in amber alongside its own period-average
 * value in the header. Hides itself when there's no Forward P/E data
 * for either company. */
export default function ForwardPEChart({
  metrics,
  metricsB,
  labelA,
  labelB,
  loadingB,
}: {
  metrics: MetricRow[];
  metricsB?: MetricRow[];
  labelA?: string;
  labelB?: string;
  /** True during B's initial metrics fetch — shows a spinner where
   * B's "Current" / "Period avg" pills will appear, instead of
   * silently rendering only A's line. */
  loadingB?: boolean;
}) {
  const dataA = useMemo(() => timeSeries(metrics, MC.FWD_PE), [metrics]);
  const dataB = useMemo(
    () => (metricsB ? timeSeries(metricsB, MC.FWD_PE) : []),
    [metricsB],
  );

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

  if (dataA.length === 0 && dataB.length === 0) {
    return <div className="text-gray-500 text-sm py-8 text-center">No Forward P/E data. Refresh to load.</div>;
  }

  return (
    <>
      <div className="text-gray-500 text-xs mb-2 flex items-center gap-2 flex-wrap">
        {latestA && (
          <>
            <span>Current {labelA ?? 'A'}:</span>
            <span className="font-mono" style={{ color: COLOR_A }}>{latestA.value.toFixed(1)}x</span>
            <span className="text-gray-600 font-mono">({latestA.date})</span>
          </>
        )}
        {hasB && latestB && (
          <>
            <span className="text-gray-700">·</span>
            <span>Current {labelB ?? 'B'}:</span>
            <span className="font-mono" style={{ color: COLOR_B }}>{latestB.value.toFixed(1)}x</span>
            <span className="text-gray-600 font-mono">({latestB.date})</span>
          </>
        )}
        {hasB && !latestB && loadingB && (
          <>
            <span className="text-gray-700">·</span>
            <span>Current {labelB ?? 'B'}:</span>
            <Spinner size={10} />
          </>
        )}
        <span className="text-gray-700">·</span>
        <span>Period avg {hasB ? (labelA ?? 'A') : ''}:</span>
        <span className="text-rose-400 font-mono">{meanA.toFixed(1)}x</span>
        {/* Period avg B is shown in-line only when comparison is active;
            we deliberately skip a second reference line on the chart
            (two dashed lines would clutter the axis). */}
        {hasB && dataB.length > 0 && (
          <>
            <span className="text-gray-700">·</span>
            <span>Period avg {labelB ?? 'B'}:</span>
            <span className="font-mono" style={{ color: COLOR_B }}>{meanB.toFixed(1)}x</span>
          </>
        )}
        {hasB && dataB.length === 0 && loadingB && (
          <>
            <span className="text-gray-700">·</span>
            <span>Period avg {labelB ?? 'B'}:</span>
            <Spinner size={10} />
          </>
        )}
        <InfoTip text="Forward P/E = Price / Next-year EPS estimate. Lower = cheaper relative to expected earnings. The red dashed line shows the average across the visible period for the primary company — useful for spotting when the stock trades above or below its typical valuation." />
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={merged} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e2330" />
          <XAxis dataKey="date" tick={{ fontSize: 10, fill: '#6b7280' }} tickFormatter={(v: string) => v.slice(0, 7)} />
          <YAxis tick={{ fontSize: 11, fill: '#6b7280' }} tickFormatter={(v: number) => `${v.toFixed(0)}x`} />
          <Tooltip
            contentStyle={tooltipStyle}
            labelStyle={{ color: '#9ca3af' }}
            formatter={(v, name) => {
              const lab = name === 'a' ? (labelA ?? 'A') : name === 'b' ? (labelB ?? 'B') : String(name);
              return [`${Number(v).toFixed(1)}x`, lab];
            }}
          />
          <ReferenceLine y={meanA} stroke="#ef4444" strokeDasharray="5 5" />
          <Line type="monotone" dataKey="a" name="a" stroke={COLOR_A} strokeWidth={2} dot={false} connectNulls />
          {hasB && <Line type="monotone" dataKey="b" name="b" stroke={COLOR_B} strokeWidth={2} dot={false} connectNulls />}
        </LineChart>
      </ResponsiveContainer>
    </>
  );
}
