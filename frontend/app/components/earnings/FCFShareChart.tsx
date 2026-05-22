'use client';

import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  ReferenceLine, CartesianGrid,
} from 'recharts';
import InfoTip from '../InfoTip';
import { MC, type MetricRow } from './types';
import { annualSeries, computeCAGR, fmtNum, fmtPct, tooltipStyle } from './utils';

/** Free Cash Flow per share over time. Negative-FCF years render with a
 * red dot. CAGR is computed only from positive values to keep the
 * compound math meaningful. */
export default function FCFShareChart({ metrics }: { metrics: MetricRow[] }) {
  const { data, cagr } = useMemo(() => {
    const series = annualSeries(metrics, MC.FCF_PS);
    if (series.length === 0) return { data: [], cagr: null };
    const positiveSeries = series.filter((s) => s.value > 0);
    return {
      data: series,
      cagr: computeCAGR(positiveSeries),
    };
  }, [metrics]);

  if (data.length === 0) {
    return <div className="text-gray-500 text-sm py-8 text-center">No FCF/share data. Refresh to load.</div>;
  }

  const hasNegative = data.some((d) => d.value < 0);

  return (
    <>
      <div className="text-gray-500 text-xs mb-2 flex items-center gap-1 flex-wrap">
        FCF per share (raw values) <InfoTip text="Free Cash Flow per share over time. Negative values are shaded red. CAGR is computed from positive values only." />
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-1 mb-2">
        <div className="flex items-center gap-1">
          <div className="text-gray-500 text-[11px]">CAGR (positive only)</div>
          <div className="text-white font-mono text-xs">{fmtPct(cagr)}</div>
        </div>
        <div className="flex items-center gap-1">
          <div className="text-gray-500 text-[11px]">Latest</div>
          <div className="text-white font-mono text-xs">{fmtNum(data[data.length - 1].value, 2)}</div>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={data} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e2330" />
          <XAxis dataKey="date" tick={{ fontSize: 10, fill: '#6b7280' }} tickFormatter={(v: string) => v.slice(0, 4)} />
          <YAxis
            tick={{ fontSize: 11, fill: '#6b7280' }}
            tickFormatter={(v: number) => v.toFixed(1)}
          />
          {hasNegative && <ReferenceLine y={0} stroke="#6b7280" strokeDasharray="3 3" />}
          <Tooltip
            contentStyle={tooltipStyle}
            labelStyle={{ color: '#9ca3af' }}
            formatter={(v) => [Number(v).toFixed(2), 'FCF/share']}
          />
          <Line
            type="monotone"
            dataKey="value"
            name="FCF/share"
            stroke="#818cf8"
            strokeWidth={2}
            dot={(props: { cx?: number; cy?: number; payload?: { value: number } }) => {
              const { cx, cy, payload } = props;
              if (payload && payload.value < 0) {
                return <circle cx={cx} cy={cy} r={3} fill="#f87171" stroke="#f87171" />;
              }
              return <circle cx={cx} cy={cy} r={0} fill="none" stroke="none" />;
            }}
          />
        </LineChart>
      </ResponsiveContainer>
    </>
  );
}
