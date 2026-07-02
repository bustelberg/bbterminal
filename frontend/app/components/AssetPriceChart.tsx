'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  Bar, CartesianGrid, ComposedChart, Line, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { chartTheme } from '../../lib/chartTheme';

type Point = { date: string; close: number | null; volume: number | null };

const fmtVol = (v: number) =>
  v >= 1e9 ? `${(v / 1e9).toFixed(2)}B` : v >= 1e6 ? `${(v / 1e6).toFixed(1)}M` : v >= 1e3 ? `${(v / 1e3).toFixed(0)}k` : `${v}`;
const fmtPrice = (v: number) => v.toLocaleString(undefined, { maximumFractionDigits: 2 });

/** Stored daily close (line) + volume (bottom-band bars) for one analysis
 * asset, from /assets/{id}/series (server-downsampled). */
export default function AssetPriceChart({ analysisId, currency }: { analysisId: number; currency: string | null }) {
  const [data, setData] = useState<Point[] | null>(null);
  const [meta, setMeta] = useState<{ total: number; points: number } | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setData(null); setError(null);
    apiFetch(`${API_URL}/api/asset-pipeline/assets/${analysisId}/series`)
      .then(async (r) => {
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) setError(b?.detail ?? `HTTP ${r.status}`);
        else { setData((b?.series ?? []) as Point[]); setMeta({ total: b.total, points: b.points }); }
      })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : String(e)); });
    return () => { cancelled = true; };
  }, [analysisId]);

  // Volume band: scale the volume axis so bars occupy only the bottom ~28%.
  const volMax = useMemo(() => Math.max(1, ...(data ?? []).map((d) => d.volume ?? 0)), [data]);

  if (error) return <div className="text-[11px] text-neg-300">Chart failed: {error}</div>;
  if (!data) return <div className="text-[11px] text-fg-faint">Loading chart…</div>;
  if (data.length === 0) return <div className="text-[11px] text-fg-faint">No stored series.</div>;

  return (
    <div>
      <div className="h-56 w-full">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
            <CartesianGrid stroke={chartTheme.grid} vertical={false} />
            <XAxis
              dataKey="date" tick={{ fontSize: 10, fill: chartTheme.axisTick }}
              tickFormatter={(d: string) => d.slice(0, 4)} minTickGap={48} tickLine={false}
              axisLine={{ stroke: chartTheme.grid }}
            />
            <YAxis
              yAxisId="price" orientation="right" width={52}
              tick={{ fontSize: 10, fill: chartTheme.axisTick }} tickLine={false}
              axisLine={false} tickFormatter={(v: number) => fmtPrice(v)}
            />
            <YAxis yAxisId="vol" hide domain={[0, volMax * 3.6]} />
            <Tooltip
              contentStyle={chartTheme.tooltipPopover.contentStyle}
              labelStyle={chartTheme.tooltipPopover.labelStyle}
              formatter={(value, name) => {
                const v = Number(value);
                if (name === 'volume') return [fmtVol(v), 'Volume'];
                return [`${fmtPrice(v)}${currency ? ` ${currency}` : ''}`, 'Close'];
              }}
            />
            <Bar yAxisId="vol" dataKey="volume" fill={chartTheme.accent} fillOpacity={0.22} isAnimationActive={false} />
            <Line yAxisId="price" type="monotone" dataKey="close" stroke={chartTheme.accentStrong} strokeWidth={1.3} dot={false} isAnimationActive={false} />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
      {meta && (
        <div className="text-[10px] text-fg-faint mt-1">
          {data.length.toLocaleString()} points shown{meta.total > meta.points ? ` (downsampled from ${meta.total.toLocaleString()})` : ''} · close + volume
        </div>
      )}
    </div>
  );
}
