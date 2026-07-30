'use client';

import { useEffect, useRef, useState } from 'react';
import {
  ColorType, createChart, HistogramSeries, LineSeries, PriceScaleMode,
  type IChartApi, type ISeriesApi, type MouseEventParams, type Time,
} from 'lightweight-charts';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { chartTheme } from '../../lib/chartTheme';

type Point = {
  date: string;
  close: number | null; volume: number | null;
  close_eur: number | null; volume_eur: number | null;
};

const fmtShares = (v: number) =>
  v >= 1e9 ? `${(v / 1e9).toFixed(2)}B` : v >= 1e6 ? `${(v / 1e6).toFixed(1)}M` : v >= 1e3 ? `${(v / 1e3).toFixed(0)}k` : `${v}`;
const fmtEur = (v: number) =>
  v >= 1e9 ? `€${(v / 1e9).toFixed(2)}B` : v >= 1e6 ? `€${(v / 1e6).toFixed(1)}M` : v >= 1e3 ? `€${(v / 1e3).toFixed(0)}k` : `€${v.toFixed(0)}`;
const fmtPrice = (v: number) => v.toLocaleString(undefined, { maximumFractionDigits: 2 });

function hexToRgba(hex: string, a: number): string {
  const h = hex.replace('#', '');
  const full = h.length === 3 ? h.split('').map((c) => c + c).join('') : h;
  const n = parseInt(full, 16);
  return `rgba(${(n >> 16) & 255},${(n >> 8) & 255},${n & 255},${a})`;
}

/** One TradingView Lightweight-Charts panel: price line (right scale) + volume
 * histogram (bottom band). Mouse-wheel / pinch zoom + drag-pan are built in;
 * double-click resets the time axis to the full range (fitContent). Price scale
 * is log or linear per `scale`. */
function LwChart({ data, priceKey, volKey, priceColor, volColor, scale, priceUnit, volFmt }: {
  data: Point[];
  priceKey: 'close' | 'close_eur';
  volKey: 'volume' | 'volume_eur';
  priceColor: string; volColor: string;
  scale: 'log' | 'linear';
  priceUnit: string;
  volFmt: (v: number) => string;
}) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const tipRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = wrapRef.current;
    const tip = tipRef.current;
    if (!el) return;

    const chart: IChartApi = createChart(el, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: chartTheme.axisTick, fontSize: 10, attributionLogo: false,
      },
      grid: { vertLines: { visible: false }, horzLines: { color: chartTheme.grid } },
      // Price axis on the LEFT so its labels reserve a gutter there — otherwise
      // the leftmost time (year) label has no room and clips off the edge.
      leftPriceScale: {
        visible: true,
        borderVisible: false,
        scaleMargins: { top: 0.08, bottom: 0.28 },
        mode: scale === 'log' ? PriceScaleMode.Logarithmic : PriceScaleMode.Normal,
      },
      rightPriceScale: { visible: false },
      // minBarSpacing default is 0.5px/bar — with ~2k downsampled points on a
      // ~900px panel, fitContent() can't fit them all and silently drops the
      // OLDEST bars off the left edge (cutting BTC's 2014-2016 history). Allow a
      // tiny spacing so the whole date range always fits.
      timeScale: { borderColor: chartTheme.grid, rightOffset: 2, minBarSpacing: 0.02 },
      handleScale: { axisDoubleClickReset: { time: true, price: true }, mouseWheel: true, pinch: true },
    });

    const line: ISeriesApi<'Line'> = chart.addSeries(LineSeries, {
      color: priceColor, lineWidth: 2, priceScaleId: 'left',
      priceLineVisible: false, lastValueVisible: false,
    });
    const vol: ISeriesApi<'Histogram'> = chart.addSeries(HistogramSeries, {
      color: hexToRgba(volColor, 0.4), priceScaleId: 'vol',
      priceLineVisible: false, lastValueVisible: false, priceFormat: { type: 'volume' },
    });
    chart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.78, bottom: 0 } });
    // Apply the price-scale mode explicitly too (belt-and-suspenders vs the
    // constructor option) so log is reliably in effect — essential for assets
    // with a huge dynamic range (e.g. BTC $457 → $100k) where a linear axis
    // pins the early years to a flat, invisible sliver at the bottom.
    chart.priceScale('left').applyOptions({
      mode: scale === 'log' ? PriceScaleMode.Logarithmic : PriceScaleMode.Normal,
    });

    const priceData: { time: Time; value: number }[] = [];
    const volData: { time: Time; value: number }[] = [];
    for (const d of data) {
      const t = d.date as unknown as Time;
      if (d[priceKey] != null) priceData.push({ time: t, value: d[priceKey] as number });
      if (d[volKey] != null) volData.push({ time: t, value: d[volKey] as number });
    }
    line.setData(priceData);
    vol.setData(volData);
    chart.timeScale().fitContent();

    const onDbl = () => chart.timeScale().fitContent();  // reset x-axis to full range
    el.addEventListener('dblclick', onDbl);

    const onMove = (param: MouseEventParams) => {
      if (!tip) return;
      if (!param.time || !param.point || param.point.x < 0 || param.point.y < 0) { tip.style.display = 'none'; return; }
      const pv = param.seriesData.get(line) as { value?: number } | undefined;
      const vv = param.seriesData.get(vol) as { value?: number } | undefined;
      tip.style.display = 'block';
      tip.innerHTML =
        `<div class="text-fg-faint">${String(param.time)}</div>` +
        (pv?.value != null ? `<div class="text-fg">${fmtPrice(pv.value)} ${priceUnit}</div>` : '') +
        (vv?.value != null ? `<div class="text-fg-subtle">${volFmt(vv.value)}</div>` : '');
      const x = Math.min(param.point.x + 12, el.clientWidth - 96);
      tip.style.left = `${Math.max(4, x)}px`;
      tip.style.top = '2px';
    };
    chart.subscribeCrosshairMove(onMove);

    return () => { el.removeEventListener('dblclick', onDbl); chart.remove(); };
  }, [data, priceKey, volKey, priceColor, volColor, scale, priceUnit, volFmt]);

  return (
    <div className="relative">
      <div ref={wrapRef} className="w-full aspect-[16/9] max-h-[72vh] min-h-[300px]" />
      <div ref={tipRef} style={{ display: 'none' }}
        className="pointer-events-none absolute z-10 bg-popover border border-neutral-800/40 rounded px-2 py-1 text-[10px] font-mono leading-tight shadow" />
    </div>
  );
}

/** Two side-by-side price+volume charts for one asset: LEFT native (as Yahoo
 * gives it — price in the listing currency, volume in shares), RIGHT converted
 * to EUR (price in EUR, volume as EUR turnover). One /series fetch feeds both.
 * Rendered with TradingView Lightweight Charts (zoom / pan / double-click reset). */
export default function AssetDualChart({ analysisId, scale = 'log' }: { analysisId: number; scale?: 'log' | 'linear' }) {
  const [data, setData] = useState<Point[] | null>(null);
  const [meta, setMeta] = useState<{ total: number; points: number; native_currency: string | null } | null>(null);
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
        else { setData((b?.series ?? []) as Point[]); setMeta({ total: b.total, points: b.points, native_currency: b.native_currency }); }
      })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : String(e)); });
    return () => { cancelled = true; };
  }, [analysisId]);

  if (error) return <div className="text-[11px] text-neg-300">Chart failed: {error}</div>;
  if (!data) return <div className="text-[11px] text-fg-faint">Loading charts…</div>;
  if (data.length === 0) return <div className="text-[11px] text-fg-faint">No stored series.</div>;

  const ccy = meta?.native_currency ?? '';
  const hasEur = data.some((d) => d.close_eur != null);

  return (
    <div className="space-y-1">
      <div className="flex gap-4 flex-wrap">
        <div className="flex-1 min-w-[320px]">
          <div className="text-[10px] uppercase tracking-wide text-fg-faint mb-1">Native{ccy ? ` (${ccy})` : ''}</div>
          <LwChart
            data={data} priceKey="close" volKey="volume" priceUnit={ccy} scale={scale}
            priceColor={chartTheme.accentStrong} volColor={chartTheme.accent} volFmt={fmtShares}
          />
        </div>
        <div className="flex-1 min-w-[320px]">
          <div className="text-[10px] uppercase tracking-wide text-fg-faint mb-1">EUR</div>
          {hasEur ? (
            <LwChart
              data={data} priceKey="close_eur" volKey="volume_eur" priceUnit="EUR" scale={scale}
              priceColor={chartTheme.pos} volColor={chartTheme.pos} volFmt={fmtEur}
            />
          ) : (
            <div className="w-full aspect-[16/9] max-h-[72vh] min-h-[300px] flex items-center justify-center text-[11px] text-fg-faint border border-neutral-800/40 rounded-lg">
              No FX rate available to convert this currency.
            </div>
          )}
        </div>
      </div>
      <div className="text-[10px] text-fg-faint">
        Left: native currency &amp; raw volume (shares). Right: prices in EUR (ECB rates), volume as EUR turnover (price×shares; crypto = converted notional).
        {' · '}Scroll to zoom, drag to pan, double-click to reset.
        {meta && meta.total > meta.points ? ` · ${data.length.toLocaleString()} of ${meta.total.toLocaleString()} bars (downsampled).` : ''}
      </div>
    </div>
  );
}
