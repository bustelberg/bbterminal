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
}: {
  subplot: YearSubplot;
  mode: Mode;
  strategyColor: string;
}) {
  // Compose flat rows for Recharts. Tooltip + dataKey resolution wants
  // primitive keys on each row, not nested {strategy, universe} objects.
  const rows = subplot.points.map((p) => ({
    date: p.date,
    strategy: p.strategyCum,
    universe: p.universeCum,
    alpha: p.alpha,
  }));

  return (
    <div className="bg-[#0f1117]/60 border border-gray-800/40 rounded-lg p-2">
      <div className="text-[11px] text-gray-400 font-mono mb-1 pl-1">
        {subplot.year}
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
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
