'use client';

import { useMemo } from 'react';
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid,
} from 'recharts';
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
      <div className="p-5 text-xs text-gray-500">No monthly data.</div>
    );
  }

  return (
    <div className="p-5">
      <div className="flex items-center justify-between mb-3">
        <div className="text-gray-400 text-xs font-medium">Monthly membership count</div>
        <div className="text-gray-600 text-[10px] font-mono">
          min {stats.min} · avg {stats.avg.toFixed(0)} · max {stats.max}
        </div>
      </div>
      <ResponsiveContainer width="100%" height={180}>
        <AreaChart data={monthly} margin={{ top: 5, right: 8, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="universeMonthlyGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#818cf8" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#818cf8" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
          <XAxis
            dataKey="month"
            tick={{ fill: '#6b7280', fontSize: 11 }}
            tickLine={false}
            interval={Math.max(0, Math.floor(monthly.length / 8) - 1)}
          />
          <YAxis
            tick={{ fill: '#6b7280', fontSize: 11 }}
            tickLine={false}
            domain={yDomain}
            allowDecimals={false}
            width={45}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1e2230',
              border: '1px solid rgba(107,114,128,0.3)',
              borderRadius: '8px',
              fontSize: 12,
            }}
            labelStyle={{ color: '#9ca3af' }}
            formatter={(value) => [Number(value ?? 0).toLocaleString(), 'Companies']}
            labelFormatter={(l) => String(l)}
          />
          <Area
            type="monotone"
            dataKey="count"
            stroke="#818cf8"
            strokeWidth={1.5}
            fill="url(#universeMonthlyGradient)"
            dot={false}
            activeDot={{ r: 4, fill: '#818cf8', stroke: '#1e2230', strokeWidth: 2 }}
          />
        </AreaChart>
      </ResponsiveContainer>
      <details className="mt-3">
        <summary className="text-gray-500 text-xs cursor-pointer hover:text-gray-300">Monthly counts ({monthly.length} months)</summary>
        <div className="mt-2 max-h-48 overflow-auto text-xs font-mono text-gray-400 grid grid-cols-3 md:grid-cols-4 gap-x-4 gap-y-1">
          {monthly.map(m => (
            <div key={m.month} className="flex justify-between">
              <span>{m.month}</span>
              <span className="text-gray-500">{m.count}</span>
            </div>
          ))}
        </div>
      </details>
    </div>
  );
}
