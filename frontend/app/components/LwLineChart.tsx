'use client';

import { useEffect, useRef } from 'react';
import {
  ColorType, createChart, LineSeries, PriceScaleMode,
  type IChartApi, type ISeriesApi, type Logical, type MouseEventParams, type Time,
} from 'lightweight-charts';
import { chartTheme } from '../../lib/chartTheme';

const fmt = (v: number) => v.toLocaleString(undefined, { maximumFractionDigits: 2 });

/** A contiguous coloured background span drawn behind the line — e.g. a
 * market-regime band. `fromIndex`/`toIndex` are logical data indices (preferred:
 * robust for off-screen / view-spanning bands); `from`/`to` ISO dates back the
 * tooltip lookup. `label` shows in the crosshair tooltip. */
export type ChartBand = { from: string; to: string; fill: string; label?: string; fromIndex?: number; toIndex?: number };

// A lightweight-charts series primitive that paints the bands on the pane, BELOW
// the line (zOrder 'bottom'), so it pans/zooms with the chart. Structurally typed
// (cast on attach) to stay resilient to minor API-name churn across versions.
class BandsPrimitive {
  private chart: IChartApi | null = null;
  private requestUpdate?: () => void;
  private readonly view: { zOrder: () => 'bottom'; renderer: () => { draw: (t: unknown) => void } };

  constructor(private bands: ChartBand[]) {
    this.view = {
      zOrder: () => 'bottom' as const,
      renderer: () => ({ draw: (target: unknown) => this.draw(target) }),
    };
  }

  private draw(target: unknown) {
    const chart = this.chart;
    if (!chart) return;
    const ts = chart.timeScale();
    (target as { useBitmapCoordinateSpace: (cb: (s: {
      context: CanvasRenderingContext2D; bitmapSize: { width: number; height: number }; horizontalPixelRatio: number;
    }) => void) => void }).useBitmapCoordinateSpace((scope) => {
      const ctx = scope.context;
      const hpr = scope.horizontalPixelRatio;
      const W = scope.bitmapSize.width;
      const H = scope.bitmapSize.height;
      for (const b of this.bands) {
        // Logical-index coordinates extrapolate off-screen, so a band that spans
        // or overhangs the viewport clamps correctly instead of vanishing.
        const c0 = b.fromIndex != null
          ? ts.logicalToCoordinate(b.fromIndex as Logical)
          : ts.timeToCoordinate(b.from as unknown as Time);
        const c1 = b.toIndex != null
          ? ts.logicalToCoordinate(b.toIndex as Logical)
          : ts.timeToCoordinate(b.to as unknown as Time);
        if (c0 == null || c1 == null) continue;
        const X0 = Math.max(0, Math.min(W, c0 * hpr));
        const X1 = Math.max(0, Math.min(W, c1 * hpr));
        if (X1 <= X0) continue;
        ctx.fillStyle = b.fill;
        ctx.fillRect(X0, 0, X1 - X0, H);
      }
    });
  }

  private req = () => this.requestUpdate?.();

  attached(p: { chart: IChartApi; requestUpdate: () => void }) {
    this.chart = p.chart;
    this.requestUpdate = p.requestUpdate;
    p.chart.timeScale().subscribeVisibleLogicalRangeChange(this.req);
  }
  detached() {
    this.chart?.timeScale().unsubscribeVisibleLogicalRangeChange(this.req);
    this.chart = null;
  }
  updateAllViews() { /* views read live state each draw */ }
  paneViews() { return [this.view]; }
}

/** A single-line TradingView Lightweight-Charts panel — the same chart engine
 * the execution-instruments page uses (mouse-wheel / pinch zoom + drag-pan;
 * double-click resets to the full range). Line-only (no volume). Optional
 * `bands` paint coloured background spans (e.g. regime bands) behind the line. */
export default function LwLineChart({ data, color = chartTheme.accentStrong, scale = 'log', unit = '', bands }: {
  data: { date: string; value: number }[];
  color?: string;
  scale?: 'log' | 'linear';
  unit?: string;
  bands?: ChartBand[];
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
        textColor: chartTheme.axisTick, fontSize: 11, attributionLogo: false,
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
    if (bands?.length) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      line.attachPrimitive(new BandsPrimitive(bands) as any);
    }
    chart.timeScale().fitContent();

    const onDbl = () => chart.timeScale().fitContent();
    el.addEventListener('dblclick', onDbl);

    // Find the regime band covering a given ISO date (for the tooltip label).
    const bandAt = (t: string): ChartBand | undefined =>
      bands?.find((b) => t >= b.from && t <= b.to);

    const onMove = (p: MouseEventParams) => {
      if (!tip) return;
      if (!p.time || !p.point || p.point.x < 0 || p.point.y < 0) { tip.style.display = 'none'; return; }
      const pv = p.seriesData.get(line) as { value?: number } | undefined;
      const band = bandAt(String(p.time));
      tip.style.display = 'block';
      tip.innerHTML = `<div class="text-fg-faint">${String(p.time)}</div>` +
        (pv?.value != null ? `<div class="text-fg">${fmt(pv.value)}${unit ? ` ${unit}` : ''}</div>` : '') +
        (band?.label ? `<div class="text-fg-muted">${band.label}</div>` : '');
      const x = Math.min(p.point.x + 12, el.clientWidth - 96);
      tip.style.left = `${Math.max(4, x)}px`;
      tip.style.top = '2px';
    };
    chart.subscribeCrosshairMove(onMove);

    return () => { el.removeEventListener('dblclick', onDbl); chart.remove(); };
  }, [data, color, scale, unit, bands]);

  return (
    <div className="relative">
      <div ref={wrapRef} className="w-full aspect-[16/9] max-h-[72vh] min-h-[300px]" />
      <div ref={tipRef} style={{ display: 'none' }}
        className="pointer-events-none absolute z-10 bg-popover border border-neutral-800/40 rounded px-2 py-1 text-[11px] font-mono leading-tight shadow" />
    </div>
  );
}
