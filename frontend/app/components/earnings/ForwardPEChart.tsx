'use client';

import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  ReferenceLine, CartesianGrid,
} from 'recharts';
import InfoTip from '../InfoTip';
import { MC, type MetricRow } from './types';
import { timeSeries, tooltipStyle } from './utils';

/** Single-series line chart of Forward P/E over time with a red dashed
 * reference line at the period average. Hides itself when there's no
 * Forward P/E data on file for the selected company. */
export default function ForwardPEChart({ metrics }: { metrics: MetricRow[] }) {
  const data = useMemo(() => timeSeries(metrics, MC.FWD_PE), [metrics]);
  const mean = useMemo(() => {
    if (data.length === 0) return 0;
    return data.reduce((s, d) => s + d.value, 0) / data.length;
  }, [data]);

  if (data.length === 0) {
    return <div className="text-gray-500 text-sm py-8 text-center">No Forward P/E data. Refresh to load.</div>;
  }

  return (
    <>
      <div className="text-gray-500 text-xs mb-2 flex items-center gap-1">Period avg: <span className="text-rose-400 font-mono">{mean.toFixed(1)}x</span> (red dashed) <InfoTip text="Forward P/E = Price / Next-year EPS estimate. Lower = cheaper relative to expected earnings. The red dashed line shows the average across the visible period — useful for spotting when the stock trades above or below its typical valuation." /></div>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={data} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e2330" />
          <XAxis dataKey="date" tick={{ fontSize: 10, fill: '#6b7280' }} tickFormatter={(v: string) => v.slice(0, 7)} />
          <YAxis tick={{ fontSize: 11, fill: '#6b7280' }} tickFormatter={(v: number) => `${v.toFixed(0)}x`} />
          <Tooltip contentStyle={tooltipStyle} labelStyle={{ color: '#9ca3af' }} formatter={(v) => [`${Number(v).toFixed(1)}x`, 'Fwd P/E']} />
          <ReferenceLine y={mean} stroke="#ef4444" strokeDasharray="5 5" />
          <Line type="monotone" dataKey="value" stroke="#818cf8" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </>
  );
}
