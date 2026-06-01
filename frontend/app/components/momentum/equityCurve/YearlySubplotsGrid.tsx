'use client';

import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import CollapsibleCard from '../CollapsibleCard';
import { tooltipStyle } from '../utils';
import type { YearSubplot } from './seriesMath';

type Mode = 'cumulative' | 'alpha';

const STRATEGY_COLOR = '#818cf8'; // indigo-400 — matches active series default
const UNIVERSE_COLOR = '#9ca3af'; // gray-400 — matches universe baseline in main chart
const ALPHA_COLOR = '#f59e0b';    // amber-500 — distinct from strategy + universe

type Props = {
  subplots: YearSubplot[];
  mode: Mode;
  /** Active strategy color from the parent's resolvedSeries. The main
   * equity chart picks color[0] for the active line; piping it in here
   * keeps the subplots visually consistent if the palette ever changes. */
  strategyColor?: string;
  /** Optional "go-live" date (YYYY-MM-DD). Drawn as a red dashed vertical
   * line in the one per-year subplot whose year contains the date, to
   * match the marker on the main equity chart. */
  markerDate?: string;
};

/** Grid of small per-year sub-charts. `mode='cumulative'` shows two
 *  rebased lines per year (strategy vs equal-weight universe). `mode='alpha'`
 *  shows a single line of arithmetic outperformance (strategy − universe in
 *  percentage points) with a zero reference line.
 *
 *  Each panel auto-scales its own y-axis so a quiet 2017 doesn't get
 *  flattened by a volatile 2020. Drawing primitives + tooltip style are
 *  shared with the main `EquityChart`. */
export default function YearlySubplotsGrid({
  subplots,
  mode,
  strategyColor = STRATEGY_COLOR,
  markerDate,
}: Props) {
  if (subplots.length === 0) return null;

  const title =
    mode === 'cumulative'
      ? 'Yearly cumulative returns vs universe'
      : 'Yearly alpha vs universe';

  return (
    <CollapsibleCard
      title={title}
      rightSlot={
        mode === 'cumulative' ? (
          <span className="flex items-center gap-3 text-[11px] text-gray-500">
            <span className="inline-flex items-center gap-1.5">
              <span className="inline-block w-2.5 h-0.5 rounded" style={{ background: strategyColor }} />
              Strategy
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="inline-block w-2.5 h-0.5 rounded" style={{ background: UNIVERSE_COLOR }} />
              Universe (equal-weight)
            </span>
          </span>
        ) : (
          <span className="flex items-center gap-1.5 text-[11px] text-gray-500">
            <span className="inline-block w-2.5 h-0.5 rounded" style={{ background: ALPHA_COLOR }} />
            Strategy − Universe (% points)
          </span>
        )
      }
      bodyClassName="px-4 pb-4"
    >
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-3">
        {subplots.map((sp) => (
          <YearMiniChart
            key={sp.year}
            subplot={sp}
            mode={mode}
            strategyColor={strategyColor}
            markerDate={markerDate}
          />
        ))}
      </div>
    </CollapsibleCard>
  );
}

function YearMiniChart({
  subplot,
  mode,
  strategyColor,
  markerDate,
}: {
  subplot: YearSubplot;
  mode: Mode;
  strategyColor: string;
  markerDate?: string;
}) {
  // Compose flat rows for Recharts. Tooltip + dataKey resolution wants
  // primitive keys on each row, not nested {strategy, universe} objects.
  const rows = subplot.points.map((p) => ({
    date: p.date,
    strategy: p.strategyCum,
    universe: p.universeCum,
    alpha: p.alpha,
  }));

  // Go-live marker: only the subplot whose year contains the date draws
  // it. Snap to the first point at/after the date (the x-axis is
  // categorical, so the ReferenceLine's `x` must equal a data point).
  // Slice to each point's own length so a YYYY-MM-DD marker matches both
  // daily (YYYY-MM-DD) and monthly (YYYY-MM) points.
  let markerX: string | null = null;
  if (markerDate && String(subplot.year) === markerDate.slice(0, 4)) {
    const hit = subplot.points.find((p) => p.date >= markerDate.slice(0, p.date.length));
    markerX = hit ? hit.date : (subplot.points[subplot.points.length - 1]?.date ?? null);
  }

  // Year-end headline: the last point that has data drives a ✓ + the figure.
  // Cumulative mode shows the strategy's return for the year (rebased to 0%
  // at year-start) with a ✓ when it finished above the universe; alpha mode
  // shows the final outperformance with a ✓ when it's positive.
  const lastValid = [...subplot.points].reverse().find((p) =>
    mode === 'cumulative'
      ? p.strategyCum != null && p.universeCum != null
      : p.alpha != null,
  );
  const headline =
    mode === 'cumulative' ? lastValid?.strategyCum ?? null : lastValid?.alpha ?? null;
  const beat =
    mode === 'cumulative'
      ? (lastValid?.strategyCum ?? 0) > (lastValid?.universeCum ?? 0)
      : (lastValid?.alpha ?? 0) > 0;

  return (
    <div className="bg-[#0f1117]/60 border border-gray-800/40 rounded-lg p-2">
      <div className="flex items-center justify-between mb-1 px-1">
        <span className="text-[11px] text-gray-400 font-mono">{subplot.year}</span>
        {headline != null && (
          <span className="flex items-center gap-1 text-[11px] font-mono">
            {beat && (
              <span
                className="text-emerald-400"
                title={mode === 'cumulative' ? 'Strategy beat the universe this year' : 'Positive alpha this year'}
              >
                ✓
              </span>
            )}
            <span className={headline >= 0 ? 'text-emerald-400' : 'text-rose-400'}>
              {`${headline >= 0 ? '+' : ''}${headline.toFixed(1)}%`}
            </span>
          </span>
        )}
      </div>
      <ResponsiveContainer width="100%" height={110}>
        <LineChart data={rows} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
          <XAxis dataKey="date" hide />
          <YAxis
            tick={{ fill: '#6b7280', fontSize: 9 }}
            tickLine={false}
            axisLine={false}
            width={28}
            tickFormatter={(v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(0)}%`}
          />
          <Tooltip
            {...tooltipStyle}
            // Compact tooltip — the panel is small, no point repeating the year.
            labelFormatter={(label) => String(label).slice(5)}
            formatter={(value, name) => {
              const v = Number(value);
              const label =
                name === 'strategy' ? 'Strategy'
                : name === 'universe' ? 'Universe'
                : 'Alpha';
              return [`${v >= 0 ? '+' : ''}${v.toFixed(2)}%`, label];
            }}
          />
          <ReferenceLine y={0} stroke="#374151" strokeDasharray="2 2" />
          {mode === 'cumulative' ? (
            <>
              <Line
                type="monotone"
                dataKey="universe"
                stroke={UNIVERSE_COLOR}
                strokeWidth={1}
                strokeDasharray="3 2"
                dot={false}
                isAnimationActive={false}
                connectNulls
              />
              <Line
                type="monotone"
                dataKey="strategy"
                stroke={strategyColor}
                strokeWidth={1.5}
                dot={false}
                isAnimationActive={false}
                connectNulls
              />
            </>
          ) : (
            <Line
              type="monotone"
              dataKey="alpha"
              stroke={ALPHA_COLOR}
              strokeWidth={1.5}
              dot={false}
              isAnimationActive={false}
              connectNulls
            />
          )}
          {markerX != null && (
            <ReferenceLine
              x={markerX}
              stroke="#ef4444"
              strokeDasharray="4 3"
              strokeWidth={1}
              ifOverflow="extendDomain"
            />
          )}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
