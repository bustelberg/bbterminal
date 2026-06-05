'use client';

import { useMemo } from 'react';
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid,
} from 'recharts';
import { chartTheme } from '../../../lib/chartTheme';
import type { MonthlyCount } from './types';

/** Monthly membership-count area chart + a collapsible per-month table.
 * Left half of an expanded universe card. */
export default function MonthlySparkline({ monthly }: { monthly: MonthlyCount[] }) {
  const stats = useMemo(() => {
    if (!monthly.length) return null;
    const counts = monthly.map(m => m.count);
    const min = Math.min(...counts);
    const max = Math.max(...counts);
    const avg = counts.reduce((a, b) => a + b, 0) / counts.length;
    return { min, max, avg };
  }, [monthly]);

  const yDomain = useMemo(() => {
    if (!stats) return [0, 1] as [number, number];
    const range = stats.max - stats.min || Math.max(stats.max, 1) * 0.1;
    const pad = range * 0.1;
    return [Math.max(0, Math.floor(stats.min - pad)), Math.ceil(stats.max + pad)] as [number, number];
  }, [stats]);

  if (!monthly.length || !stats) {
    return (
      <div className="p-5 text-xs text-fg-subtle">No monthly data.</div>
    );
  }

  return (
    <div className="p-5">
      <div className="flex items-center justify-between mb-3">
        <div className="text-fg-muted text-xs font-medium">Monthly membership count</div>
        <div className="text-fg-faint text-[10px] font-mono">
          min {stats.min} · avg {stats.avg.toFixed(0)} · max {stats.max}
        </div>
      </div>
      <ResponsiveContainer width="100%" height={180}>
        <AreaChart data={monthly} margin={{ top: 5, right: 8, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="universeMonthlyGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={chartTheme.accent} stopOpacity={0.3} />
              <stop offset="95%" stopColor={chartTheme.accent} stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.grid} />
          <XAxis
            dataKey="month"
            tick={{ fill: chartTheme.axisTick, fontSize: 11 }}
            tickLine={false}
            interval={Math.max(0, Math.floor(monthly.length / 8) - 1)}
          />
          <YAxis
            tick={{ fill: chartTheme.axisTick, fontSize: 11 }}
            tickLine={false}
            domain={yDomain}
            allowDecimals={false}
            width={45}
          />
          <Tooltip
            contentStyle={chartTheme.tooltipPopover.contentStyle}
            labelStyle={chartTheme.tooltipPopover.labelStyle}
            formatter={(value) => [Number(value ?? 0).toLocaleString(), 'Companies']}
            labelFormatter={(l) => String(l)}
          />
          <Area
            type="monotone"
            dataKey="count"
            stroke={chartTheme.accent}
            strokeWidth={1.5}
            fill="url(#universeMonthlyGradient)"
            dot={false}
            activeDot={{ r: 4, fill: chartTheme.accent, stroke: chartTheme.tooltipPopover.contentStyle.backgroundColor, strokeWidth: 2 }}
          />
        </AreaChart>
      </ResponsiveContainer>
      <details className="mt-3">
        <summary className="text-fg-subtle text-xs cursor-pointer hover:text-fg-soft">Monthly counts ({monthly.length} months)</summary>
        <div className="mt-2 max-h-48 overflow-auto text-xs font-mono text-fg-muted grid grid-cols-3 md:grid-cols-4 gap-x-4 gap-y-1">
          {monthly.map(m => (
            <div key={m.month} className="flex justify-between">
              <span>{m.month}</span>
              <span className="text-fg-subtle">{m.count}</span>
            </div>
          ))}
        </div>
      </details>
    </div>
  );
}
