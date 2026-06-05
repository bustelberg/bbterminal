'use client';

import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, Legend, ReferenceArea, ReferenceLine,
} from 'recharts';
import CollapsibleCard from '../CollapsibleCard';
import { tooltipStyle } from '../utils';
import { chartTheme } from '../../../../lib/chartTheme';
import type { AlignedResult } from './seriesMath';

/** Cumulative-return line chart with log-scale toggle + drawdown
 * overlays for the active strategy's top 3 drawdowns. One line per
 * series (active first, comparisons after), each color-keyed against
 * the comparison panel above. */
export default function EquityChart({
  displayChartData,
  alignedSeries,
  chartYDomain,
  logScale,
  setLogScale,
  hoveredDrawdown,
  setHoveredDrawdown,
  markerDate,
}: {
  displayChartData: Record<string, string | number | null>[];
  alignedSeries: AlignedResult;
  chartYDomain: [number, number];
  logScale: boolean;
  setLogScale: (v: boolean) => void;
  hoveredDrawdown: number | null;
  setHoveredDrawdown: (v: number | null) => void;
  /** Optional "go-live" date (YYYY-MM-DD) drawn as a red dashed vertical
   * line. Snapped to the nearest chart x-value at/after the date so it
   * renders on this category axis even when the exact day isn't a point. */
  markerDate?: string;
}) {
  // The x-axis is categorical (date strings), so a ReferenceLine's `x`
  // must equal an existing data point's date. Snap the marker to the first
  // chart date at/after it (or the last point when it's past the curve's
  // end). Dates are ISO so lexical comparison is chronological; slicing to
  // the data point's own length lets a YYYY-MM-DD marker match YYYY-MM
  // monthly points too.
  const markerX = useMemo<string | null>(() => {
    if (!markerDate || displayChartData.length === 0) return null;
    for (const row of displayChartData) {
      const d = row.date;
      if (typeof d !== 'string') continue;
      if (d >= markerDate.slice(0, d.length)) return d;
    }
    const last = displayChartData[displayChartData.length - 1]?.date;
    return typeof last === 'string' ? last : null;
  }, [markerDate, displayChartData]);

  return (
    <CollapsibleCard
      title={`Equity Curve (${logScale ? 'Log' : 'Cumulative'} Return %)`}
      rightSlot={
        <label
          className="flex items-center gap-2 cursor-pointer select-none"
          onClick={(e) => e.stopPropagation()}
        >
          <input
            type="checkbox"
            checked={logScale}
            onChange={(e) => setLogScale(e.target.checked)}
            className="accent-accent-500 w-3.5 h-3.5"
          />
          Log scale
        </label>
      }
      bodyClassName="px-5 pb-5"
    >
      <ResponsiveContainer width="100%" height={350}>
        <LineChart data={displayChartData}>
          <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.grid} />
          <XAxis
            dataKey="date"
            tick={{ fill: chartTheme.axisTick, fontSize: 11 }}
            tickLine={false}
            interval={Math.max(0, Math.floor(displayChartData.length / 12) - 1)}
          />
          <YAxis
            tick={{ fill: chartTheme.axisTick, fontSize: 11 }}
            tickLine={false}
            tickFormatter={(v: number) => `${v}%`}
            domain={chartYDomain}
          />
          <Tooltip
            {...tooltipStyle}
            formatter={(value, name) => {
              const v = Number(value);
              const s = alignedSeries.series.find((x) => x.id === name);
              return [`${v >= 0 ? '+' : ''}${v.toFixed(2)}%`, s?.label ?? String(name)];
            }}
          />
          {alignedSeries.series.length > 1 && (
            <Legend
              wrapperStyle={{ fontSize: 12, color: chartTheme.axisLabel }}
              formatter={(value) => {
                const s = alignedSeries.series.find((x) => x.id === value);
                return s?.label ?? String(value);
              }}
            />
          )}
          {/* Drawdown overlays: only for the active strategy (first series) */}
          {alignedSeries.series[0]?.topDrawdowns.map((dd, i) => {
            const base = [0.25, 0.15, 0.10];
            const hovered = hoveredDrawdown === i;
            const opacity = hovered ? (base[i] ?? 0.10) + 0.15 : (base[i] ?? 0.10);
            return (
              <ReferenceArea
                key={`dd-${i}`}
                x1={dd.peak_date}
                x2={dd.recovery_date ?? (displayChartData[displayChartData.length - 1]?.date as string | undefined)}
                y1={chartYDomain[0]}
                y2={chartYDomain[1]}
                fill={chartTheme.drawdown(opacity)}
                strokeOpacity={0}
                style={{ cursor: 'pointer' }}
                onMouseEnter={() => setHoveredDrawdown(i)}
                onMouseLeave={() => setHoveredDrawdown(null)}
              />
            );
          })}
          {alignedSeries.series.map((s, i) => (
            <Line
              key={s.id}
              type="monotone"
              dataKey={s.id}
              stroke={s.color}
              strokeWidth={i === 0 ? 2 : 1.5}
              strokeDasharray={i === 0 ? undefined : '4 3'}
              dot={false}
              name={s.id}
              connectNulls
            />
          ))}
          {markerX != null && (
            <ReferenceLine
              x={markerX}
              stroke={chartTheme.goLiveLine}
              strokeDasharray="5 5"
              strokeWidth={1.5}
              ifOverflow="extendDomain"
              label={{ value: 'Go-live', position: 'insideTopRight', fill: chartTheme.goLiveLabel, fontSize: 10 }}
            />
          )}
        </LineChart>
      </ResponsiveContainer>
    </CollapsibleCard>
  );
}
