'use client';

import { useEffect, useRef } from 'react';
import {
  ColorType, createChart, LineSeries, PriceScaleMode,
  type IChartApi, type ISeriesApi, type MouseEventParams, type Time,
} from 'lightweight-charts';
import { chartTheme } from '../../lib/chartTheme';

const fmt = (v: number) => v.toLocaleString(undefined, { maximumFractionDigits: 2 });

/** A single-line TradingView Lightweight-Charts panel — the same chart engine
 * the execution-instruments page uses (mouse-wheel / pinch zoom + drag-pan;
 * double-click resets to the full range). Line-only (no volume). */
export default function LwLineChart({ data, color = chartTheme.accentStrong, scale = 'log', unit = '' }: {
  data: { date: string; value: number }[];
  color?: string;
  scale?: 'log' | 'linear';
  unit?: string;
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
      leftPriceScale: {
        visible: true, borderVisible: false, scaleMargins: { top: 0.08, bottom: 0.08 },
        mode: scale === 'log' ? PriceScaleMode.Logarithmic : PriceScaleMode.Normal,
      },
      rightPriceScale: { visible: false },
      timeScale: { borderColor: chartTheme.grid, rightOffset: 2, minBarSpacing: 0.02 },
      handleScale: { axisDoubleClickReset: { time: true, price: true }, mouseWheel: true, pinch: true },
    });

    const line: ISeriesApi<'Line'> = chart.addSeries(LineSeries, {
      color, lineWidth: 2, priceScaleId: 'left', priceLineVisible: false, lastValueVisible: false,
    });
    chart.priceScale('left').applyOptions({
      mode: scale === 'log' ? PriceScaleMode.Logarithmic : PriceScaleMode.Normal,
    });
    line.setData(
      data.filter((d) => d.value != null).map((d) => ({ time: d.date as unknown as Time, value: d.value })),
    );
    chart.timeScale().fitContent();

    const onDbl = () => chart.timeScale().fitContent();
    el.addEventListener('dblclick', onDbl);

    const onMove = (p: MouseEventParams) => {
      if (!tip) return;
      if (!p.time || !p.point || p.point.x < 0 || p.point.y < 0) { tip.style.display = 'none'; return; }
      const pv = p.seriesData.get(line) as { value?: number } | undefined;
      tip.style.display = 'block';
      tip.innerHTML = `<div class="text-fg-faint">${String(p.time)}</div>` +
        (pv?.value != null ? `<div class="text-fg">${fmt(pv.value)}${unit ? ` ${unit}` : ''}</div>` : '');
      const x = Math.min(p.point.x + 12, el.clientWidth - 96);
      tip.style.left = `${Math.max(4, x)}px`;
      tip.style.top = '2px';
    };
    chart.subscribeCrosshairMove(onMove);

    return () => { el.removeEventListener('dblclick', onDbl); chart.remove(); };
  }, [data, color, scale, unit]);

  return (
    <div className="relative">
      <div ref={wrapRef} className="w-full aspect-[16/9] max-h-[72vh] min-h-[300px]" />
      <div ref={tipRef} style={{ display: 'none' }}
        className="pointer-events-none absolute z-10 bg-popover border border-neutral-800/40 rounded px-2 py-1 text-[10px] font-mono leading-tight shadow" />
    </div>
  );
}
