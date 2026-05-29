'use client';

import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  ReferenceLine, CartesianGrid,
} from 'recharts';
import InfoTip from '../InfoTip';
import Spinner from '../Spinner';
import { MC, type MetricRow } from './types';
import { annualSeries, computeCAGR, fmtNum, fmtPct, tooltipStyle } from './utils';

// Series colors — must match ForwardPEChart and RelativeGrowthChart's
// A/B treatment so the user reads them consistently across panels.
const COLOR_A = '#818cf8'; // indigo-400
const COLOR_B = '#f59e0b'; // amber-500

type Props = {
  metrics: MetricRow[];
  metricsB?: MetricRow[];
  labelA?: string;
  labelB?: string;
  /** True during B's initial metrics fetch. Shows a spinner in the
   * "CAGR / Latest" pill area for B so the comparison column doesn't
   * just sit blank while data is loading. */
  loadingB?: boolean;
};

/** Free Cash Flow per share over time. Negative-FCF years render with a
 * red dot. CAGR is computed only from positive values to keep the
 * compound math meaningful. With a comparison company, the second
 * series renders in amber + its own CAGR / Latest pills appear in the
 * header. */
export default function FCFShareChart({ metrics, metricsB, labelA, labelB, loadingB }: Props) {
  const seriesA = useMemo(() => annualSeries(metrics, MC.FCF_PS), [metrics]);
  const seriesB = useMemo(
    () => (metricsB ? annualSeries(metricsB, MC.FCF_PS) : []),
    [metricsB],
  );
  const cagrA = useMemo(() => computeCAGR(seriesA.filter((s) => s.value > 0)), [seriesA]);
  const cagrB = useMemo(() => computeCAGR(seriesB.filter((s) => s.value > 0)), [seriesB]);

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

  if (seriesA.length === 0 && seriesB.length === 0) {
    return <div className="text-gray-500 text-sm py-8 text-center">No FCF/share data. Refresh to load.</div>;
  }

  return (
    <>
      <div className="text-gray-500 text-xs mb-2 flex items-center gap-1 flex-wrap">
        FCF per share (raw values) <InfoTip text="Free Cash Flow per share over time. Negative values are shaded red. CAGR is computed from positive values only." />
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-1 mb-2">
        <div className="flex items-center gap-1">
          <div className="text-[11px]" style={{ color: COLOR_A }}>CAGR {labelA ?? 'A'} (positive only)</div>
          <div className="font-mono text-xs" style={{ color: COLOR_A }}>{fmtPct(cagrA)}</div>
        </div>
        <div className="flex items-center gap-1">
          <div className="text-gray-500 text-[11px]">Latest {labelA ?? 'A'}</div>
          <div className="font-mono text-xs" style={{ color: COLOR_A }}>{fmtNum(latestA, 2)}</div>
        </div>
        {hasB && seriesB.length > 0 && (
          <>
            <div className="flex items-center gap-1">
              <div className="text-[11px]" style={{ color: COLOR_B }}>CAGR {labelB ?? 'B'} (positive only)</div>
              <div className="font-mono text-xs" style={{ color: COLOR_B }}>{fmtPct(cagrB)}</div>
            </div>
            <div className="flex items-center gap-1">
              <div className="text-gray-500 text-[11px]">Latest {labelB ?? 'B'}</div>
              <div className="font-mono text-xs" style={{ color: COLOR_B }}>{fmtNum(latestB, 2)}</div>
            </div>
          </>
        )}
        {hasB && seriesB.length === 0 && loadingB && (
          <div className="flex items-center gap-1.5">
            <div className="text-gray-500 text-[11px]">Loading {labelB ?? 'B'}</div>
            <Spinner size={10} />
          </div>
        )}
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={merged} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
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
            formatter={(v, name) => {
              const lab = name === 'a' ? (labelA ?? 'A') : name === 'b' ? (labelB ?? 'B') : String(name);
              return [Number(v).toFixed(2), lab];
            }}
          />
          <Line
            type="monotone"
            dataKey="a"
            name="a"
            stroke={COLOR_A}
            strokeWidth={2}
            connectNulls
            dot={(props: { cx?: number; cy?: number; payload?: { a?: number } }) => {
              const { cx, cy, payload } = props;
              if (payload && payload.a != null && payload.a < 0) {
                return <circle cx={cx} cy={cy} r={3} fill="#f87171" stroke="#f87171" />;
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
              dot={(props: { cx?: number; cy?: number; payload?: { b?: number } }) => {
                const { cx, cy, payload } = props;
                if (payload && payload.b != null && payload.b < 0) {
                  return <circle cx={cx} cy={cy} r={3} fill="#dc2626" stroke="#dc2626" />;
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
