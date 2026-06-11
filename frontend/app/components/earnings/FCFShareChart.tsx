'use client';

import { memo, useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  ReferenceLine, CartesianGrid,
} from 'recharts';
import InfoTip from '../InfoTip';
import Spinner from '../Spinner';
import { MC, type MetricRow } from './types';
import { annualSeries, computeCAGR, fmtNum, fmtPct, tooltipStyle } from './utils';
import { buildMemberSeries, weightedAverageSeries, type PortfolioMemberMetrics } from './portfolioBreakdown';
import { chartTheme } from '../../../lib/chartTheme';

// Series colors — must match ForwardPEChart and RelativeGrowthChart's
// A/B treatment so the user reads them consistently across panels.
const COLOR_A = chartTheme.accent; // primary series
const COLOR_B = chartTheme.compare;   // comparison series (violet — not a band colour)

type Props = {
  metrics: MetricRow[];
  metricsB?: MetricRow[];
  labelA?: string;
  labelB?: string;
  /** Real company name shown in the hover tooltip (vs the short A/B pill tag). */
  nameA?: string;
  nameB?: string;
  /** Convert a native-currency value (as of its date) to EUR, so A and B
   * compare directly. Defaults to identity (values shown as-is). */
  toEurA?: (value: number, date: string) => number;
  toEurB?: (value: number, date: string) => number;
  /** True during B's initial metrics fetch. Shows a spinner in the
   * "CAGR / Latest" pill area for B so the comparison column doesn't
   * just sit blank while data is loading. */
  loadingB?: boolean;
  /** Per-member metrics when a side is a portfolio (already EUR-converted) —
   * the line is then the weighted-member average, smooth instead of the raw
   * fiscal-misaligned blend. */
  breakdownA?: PortfolioMemberMetrics[];
  breakdownB?: PortfolioMemberMetrics[];
};

const IDENTITY = (v: number) => v;

/** Free Cash Flow per share over time. Negative-FCF years render with a
 * red dot. CAGR is computed only from positive values to keep the
 * compound math meaningful. With a comparison company, the second
 * series renders in amber + its own CAGR / Latest pills appear in the
 * header. */
function FCFShareChartInner({ metrics, metricsB, labelA, labelB, nameA, nameB, toEurA = IDENTITY, toEurB = IDENTITY, loadingB, breakdownA, breakdownB }: Props) {
  // FCF/share is reported in the company's native currency; convert each
  // year's value to EUR (at that year's FX rate) so A and B compare directly.
  // A portfolio's line is the weighted average of its holdings' (already-EUR)
  // FCF/share series — no re-conversion, and no fiscal-misalignment spikes.
  const seriesA = useMemo(
    () => (breakdownA
      ? weightedAverageSeries(buildMemberSeries(breakdownA, (m) => annualSeries(m, MC.FCF_PS)))
      : annualSeries(metrics, MC.FCF_PS).map((p) => ({ date: p.date, value: toEurA(p.value, p.date) }))),
    [breakdownA, metrics, toEurA],
  );
  const seriesB = useMemo(
    () => (breakdownB
      ? weightedAverageSeries(buildMemberSeries(breakdownB, (m) => annualSeries(m, MC.FCF_PS)))
      : (metricsB ? annualSeries(metricsB, MC.FCF_PS).map((p) => ({ date: p.date, value: toEurB(p.value, p.date) })) : [])),
    [breakdownB, metricsB, toEurB],
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
  // Series tag is shown only in comparison mode (generic A/B, never the
  // company ticker). In single-company mode the pills read just "Latest".
  const aTag = hasB ? ` ${labelA ?? 'A'}` : '';
  const hasNegative = merged.some((d) => (d.a != null && d.a < 0) || (d.b != null && d.b < 0));
  const latestA = seriesA.length > 0 ? seriesA[seriesA.length - 1].value : null;
  const latestB = seriesB.length > 0 ? seriesB[seriesB.length - 1].value : null;

  if (seriesA.length === 0 && seriesB.length === 0) {
    return <div className="text-fg-subtle text-sm py-8 text-center">No FCF/share data. Refresh to load.</div>;
  }

  return (
    <>
      <div className="text-fg-subtle text-xs mb-2 flex items-center gap-1 flex-wrap">
        FCF per share (€) <InfoTip text="Free Cash Flow per share over time, converted to EUR (at each year's FX rate) so companies compare directly. Negative values are shaded red. CAGR is computed from positive values only." />
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-1 mb-2">
        <div className="flex items-center gap-1">
          <div className="text-[11px]" style={{ color: COLOR_A }}>CAGR{aTag} (positive only)</div>
          <div className="font-mono text-xs" style={{ color: COLOR_A }}>{fmtPct(cagrA)}</div>
        </div>
        <div className="flex items-center gap-1">
          <div className="text-fg-subtle text-[11px]">Latest{aTag}</div>
          <div className="font-mono text-xs" style={{ color: COLOR_A }}>{latestA == null ? '—' : `€${fmtNum(latestA, 2)}`}</div>
        </div>
        {hasB && seriesB.length > 0 && (
          <>
            <div className="flex items-center gap-1">
              <div className="text-[11px]" style={{ color: COLOR_B }}>CAGR {labelB ?? 'B'} (positive only)</div>
              <div className="font-mono text-xs" style={{ color: COLOR_B }}>{fmtPct(cagrB)}</div>
            </div>
            <div className="flex items-center gap-1">
              <div className="text-fg-subtle text-[11px]">Latest {labelB ?? 'B'}</div>
              <div className="font-mono text-xs" style={{ color: COLOR_B }}>{latestB == null ? '—' : `€${fmtNum(latestB, 2)}`}</div>
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
          <XAxis dataKey="date" tick={{ fontSize: 10, fill: chartTheme.axisTick }} tickFormatter={(v: string) => v.slice(0, 4)} />
          <YAxis
            tick={{ fontSize: 11, fill: chartTheme.axisTick }}
            tickFormatter={(v: number) => `€${v.toFixed(1)}`}
          />
          {hasNegative && <ReferenceLine y={0} stroke={chartTheme.axisTick} strokeDasharray="3 3" />}
          <Tooltip
            contentStyle={tooltipStyle}
            labelStyle={{ color: chartTheme.axisLabel }}
            formatter={(v, name) => {
              const lab = name === 'a' ? (nameA ?? labelA ?? 'A') : name === 'b' ? (nameB ?? labelB ?? 'B') : String(name);
              return [`€${Number(v).toFixed(2)}`, lab];
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
                return <circle cx={cx} cy={cy} r={3} fill={chartTheme.neg} stroke={chartTheme.neg} />;
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
                  return <circle cx={cx} cy={cy} r={3} fill={chartTheme.negDeep} stroke={chartTheme.negDeep} />;
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

// Memoized — see MetricBandChart: avoids re-rendering on SSE-log churn during refresh.
const FCFShareChart = memo(FCFShareChartInner);
export default FCFShareChart;
