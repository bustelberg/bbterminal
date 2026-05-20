'use client';

import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, Legend, ReferenceArea,
} from 'recharts';
import CollapsibleCard from '../CollapsibleCard';
import { tooltipStyle } from '../utils';
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
}: {
  displayChartData: Record<string, string | number | null>[];
  alignedSeries: AlignedResult;
  chartYDomain: [number, number];
  logScale: boolean;
  setLogScale: (v: boolean) => void;
  hoveredDrawdown: number | null;
  setHoveredDrawdown: (v: number | null) => void;
}) {
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
            className="accent-indigo-500 w-3.5 h-3.5"
          />
          Log scale
        </label>
      }
      bodyClassName="px-5 pb-5"
    >
      <ResponsiveContainer width="100%" height={350}>
        <LineChart data={displayChartData}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
          <XAxis
            dataKey="date"
            tick={{ fill: '#6b7280', fontSize: 11 }}
            tickLine={false}
            interval={Math.max(0, Math.floor(displayChartData.length / 12) - 1)}
          />
          <YAxis
            tick={{ fill: '#6b7280', fontSize: 11 }}
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
              wrapperStyle={{ fontSize: 12, color: '#9ca3af' }}
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
                fill={`rgba(244,63,94,${opacity})`}
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
        </LineChart>
      </ResponsiveContainer>
    </CollapsibleCard>
  );
}
