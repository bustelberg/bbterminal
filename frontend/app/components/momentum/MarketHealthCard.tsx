'use client';

import { useEffect, useMemo, useState } from 'react';
import type { MouseEvent as ReactMouseEvent } from 'react';

import CollapsibleCard from './CollapsibleCard';
import { useBenchmarks } from '../../../lib/hooks/apiData';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import type { BacktestResult } from '../../../lib/stores/momentum';
import type { BenchmarkOption, BenchmarkPrice } from './types';

/**
 * Market-health signal chart — only rendered when the regime filter was
 * active on this run (i.e. some period carries a `market_health` score).
 * For manual investigation: it overlays the raw composite health signal
 * (trend + 6-mo momentum + drawdown breadth, 0..1) against the resulting
 * book exposure each rebalance, with the ramp band (lo→hi) shaded.
 *
 * Below it, an optional benchmark price chart shares the SAME time axis,
 * so you can eyeball whether health dips line up with benchmark crises
 * (e.g. pick SP500 and see if the health line sagged during its crashes).
 *
 * Pure SVG (mirrors the rolling-correlation chart in DailyReturnsHistograms)
 * — no recharts dependency. A single hovered date is shared across both
 * charts so their vertical guides stay in lockstep.
 */

// Default ramp endpoints (BacktestConfig.regime_ramp_lo/hi). Not surfaced
// per-record; the UI never changes them, so the band is drawn at the
// defaults. If you tune them via the API the shaded band is indicative.
const RAMP_LO = 0.3;
const RAMP_HI = 0.7;

const HEALTH_COLOR = 'var(--color-accent-500)';
const EXPOSURE_COLOR = 'var(--color-warn-500)';
const BENCH_COLOR = 'var(--color-fg-muted)';
// Component sub-signal lines (shown on the Regime Detector page).
const TREND_COLOR = 'var(--color-pos-500)';
const MOM_COLOR = 'var(--color-fg-muted)';
const DD_COLOR = 'var(--color-neg-400)';
const RSI_SIMPLE_COLOR = 'var(--color-accent-400)';
const RSI_WILDER_COLOR = 'var(--color-warn-500)';

// Shared horizontal geometry so both charts map a date to the SAME x.
const CHART_W = 200;
const PAD_X = 2;
const INNER_W = CHART_W - 2 * PAD_X;

type HealthPoint = {
  date: string;
  health: number;
  exposure: number;
  trend?: number;
  momentum?: number;
  drawdown?: number;
};
type BenchPoint = { date: string; price: number; ddPct: number };

/** Continuous tooltip anchor so it never spills past the chart edges:
 * left-aligned at the start, centred in the middle, right-aligned at the
 * end — instead of three discrete buckets that overshoot near the edge.
 * Horizontal only — the tooltip is pinned just below the chart's top edge
 * (see the `top-1` class) and rendered DOWNWARD into the chart so the
 * card's paint-containment / overflow-hidden can never clip it (which it
 * would if the tooltip extended upward above the chart). */
function tipTransform(leftFrac: number): string {
  return `translateX(${(-leftFrac * 100).toFixed(1)}%)`;
}

export default function MarketHealthCard({
  result,
  defaultCollapsed = false,
  showComponents = false,
}: {
  result: BacktestResult;
  defaultCollapsed?: boolean;
  /** Overlay the per-component sub-signals (trend / momentum / drawdown).
   * On by the Regime Detector page; off in the backtest result view. */
  showComponents?: boolean;
}) {
  const points = useMemo<HealthPoint[]>(() => {
    const out: HealthPoint[] = [];
    for (const r of result.monthly_records ?? []) {
      if (r.market_health == null) continue;
      const c = r.market_health_components;
      out.push({
        date: r.date.slice(0, 10),
        health: r.market_health,
        exposure: r.exposure_scale ?? 1.0, // missing → fully invested
        ...(showComponents && c
          ? { trend: c.trend, momentum: c.momentum, drawdown: c.drawdown }
          : {}),
      });
    }
    return out;
  }, [result, showComponents]);

  // One hovered date shared by both charts (guides stay aligned).
  const [hoverDate, setHoverDate] = useState<string | null>(null);

  // Benchmark overlay state.
  const { data: benchmarks } = useBenchmarks();
  const options = useMemo<BenchmarkOption[]>(() => (benchmarks ?? []) as BenchmarkOption[], [benchmarks]);
  const [benchId, setBenchId] = useState<number | null>(null);
  const [benchPrices, setBenchPrices] = useState<BenchmarkPrice[]>([]);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    if (benchId == null) { setBenchPrices([]); return; }
    let cancelled = false;
    apiFetch(`${API_URL}/api/benchmarks/${benchId}/prices?start_date=1990-01-01&end_date=2099-12-31`)
      .then((r) => (r.ok ? r.json() : []))
      .then((d: BenchmarkPrice[]) => { if (!cancelled) setBenchPrices(Array.isArray(d) ? d : []); })
      .catch(() => { if (!cancelled) setBenchPrices([]); });
    return () => { cancelled = true; };
  }, [benchId]);

  // No regime filter on this run → render nothing.
  if (points.length < 2) return null;

  const times = points.map((p) => Date.parse(p.date));
  const minMs = Math.min(...times);
  const maxMs = Math.max(...times);
  const span = maxMs - minMs || 1;
  const xOf = (date: string) => PAD_X + ((Date.parse(date) - minMs) / span) * INNER_W;

  // Benchmark series clipped to the health window + running-peak drawdown
  // (so the tooltip can quantify how deep each dip is).
  const benchPoints: BenchPoint[] = (() => {
    const inRange = benchPrices
      .filter((p) => {
        const t = Date.parse(p.target_date);
        return t >= minMs && t <= maxMs;
      })
      .sort((a, b) => a.target_date.localeCompare(b.target_date));
    const out: BenchPoint[] = [];
    let peak = -Infinity;
    for (const p of inRange) {
      if (p.price > peak) peak = p.price;
      out.push({
        date: p.target_date.slice(0, 10),
        price: p.price,
        ddPct: peak > 0 ? (p.price / peak - 1) * 100 : 0,
      });
    }
    return out;
  })();

  // Universe-average RSI(14) time series (Regime Detector only) — both the
  // simple (SMA) and Wilder-smoothed forms.
  const rsiPoints: { date: string; simple: number | null; wilder: number | null }[] = showComponents
    ? (result.monthly_records ?? [])
        .filter((r) => r.universe_rsi != null)
        .map((r) => ({
          date: r.date.slice(0, 10),
          simple: r.universe_rsi?.simple ?? null,
          wilder: r.universe_rsi?.wilder ?? null,
        }))
    : [];

  const benchName =
    options.find((o) => o.benchmark_id === benchId)?.ticker ??
    options.find((o) => o.benchmark_id === benchId)?.name ??
    'Benchmark';

  return (
    <CollapsibleCard
      title="Market-health signal"
      defaultCollapsed={defaultCollapsed}
      rightSlot={
        <span className="text-[11px] font-mono text-fg-faint">
          {points[0].date} → {points[points.length - 1].date}
        </span>
      }
      bodyClassName="px-5 py-4 space-y-3"
    >
      <HealthChart points={points} xOf={xOf} hoverDate={hoverDate} setHoverDate={setHoverDate} showComponents={showComponents} />
      <div className="flex flex-wrap justify-center gap-x-6 gap-y-1">
        <Legend color={HEALTH_COLOR} label="Composite health" />
        {showComponents && (
          <>
            <Legend color={TREND_COLOR} label="› trend (above 200-MA)" />
            <Legend color={MOM_COLOR} label="› momentum (6-mo > 0)" />
            <Legend color={DD_COLOR} label="› drawdown (near highs)" />
          </>
        )}
        <Legend color={EXPOSURE_COLOR} label="Resulting exposure (×book)" />
      </div>
      <p className="text-[10px] text-fg-faint">
        Health is the average of three absolute breadth measures across the eligible universe, measured strictly before each rebalance.
        Exposure ramps from the floor when health ≤ {RAMP_LO} up to 100% when health ≥ {RAMP_HI} (shaded band), proportional in between.
        Where the orange line sits below 1.0, the filter held cash that period.
      </p>

      {/* Universe-average RSI(14) — a separate momentum-breadth series on
          the same time axis. */}
      {showComponents && rsiPoints.length >= 2 && (
        <div className="pt-3 border-t border-neutral-800/40 space-y-2">
          <span className="text-[11px] uppercase tracking-wide text-fg-faint">Universe RSI(14) · average across all names</span>
          <RsiChart points={rsiPoints} xOf={xOf} hoverDate={hoverDate} setHoverDate={setHoverDate} />
          <div className="flex flex-wrap justify-center gap-x-6 gap-y-1">
            <Legend color={RSI_SIMPLE_COLOR} label="Simple (SMA)" />
            <Legend color={RSI_WILDER_COLOR} label="Wilder (smoothed)" />
          </div>
          <p className="text-[10px] text-fg-faint">
            Mean 14-day RSI over every company in the universe. Classic bands: &gt;70 overbought (broadly extended), &lt;30 oversold (broadly washed out). Wilder&apos;s smoothing reacts slower than the simple average — the gap shows how much lag the smoothing adds.
          </p>
        </div>
      )}

      {/* Benchmark overlay — same time axis so crises line up vertically. */}
      <div className="pt-3 border-t border-neutral-800/40 space-y-2">
        <div className="flex items-center justify-between gap-2">
          <span className="text-[11px] uppercase tracking-wide text-fg-faint">Benchmark (aligned · log scale)</span>
          <label className="flex items-center gap-1.5 text-[11px] text-fg-muted">
            Series
            <select
              value={benchId ?? ''}
              onChange={(e) => setBenchId(e.target.value ? Number(e.target.value) : null)}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-0.5 text-[11px] text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
            >
              <option value="">(pick one)</option>
              {options.map((o) => (
                <option key={o.benchmark_id} value={o.benchmark_id}>{o.ticker} — {o.name}</option>
              ))}
            </select>
          </label>
        </div>
        {benchId == null ? (
          <div className="text-[11px] text-fg-subtle">Pick a benchmark (e.g. SP500) to overlay its price on the same dates — dips should line up with health sags.</div>
        ) : benchPoints.length < 2 ? (
          <div className="text-[11px] text-fg-subtle">No {benchName} prices in this date range.</div>
        ) : (
          <BenchmarkChart
            name={benchName}
            points={benchPoints}
            xOf={xOf}
            minMs={minMs}
            span={span}
            hoverDate={hoverDate}
            setHoverDate={setHoverDate}
          />
        )}
      </div>
    </CollapsibleCard>
  );
}

function HealthChart({
  points, xOf, hoverDate, setHoverDate, showComponents,
}: {
  points: HealthPoint[];
  xOf: (date: string) => number;
  hoverDate: string | null;
  setHoverDate: (d: string | null) => void;
  showComponents: boolean;
}) {
  const H = 90, padTop = 4, padBot = 4;
  const innerH = H - padTop - padBot;
  const yOf = (v: number) => padTop + (1 - v) * innerH; // v=1 top, 0 bottom

  const times = points.map((p) => Date.parse(p.date));
  const minMs = Math.min(...times), maxMs = Math.max(...times);
  const fmtDate = (ms: number) => new Date(ms).toISOString().slice(0, 10);

  const linePath = (key: 'health' | 'exposure' | 'trend' | 'momentum' | 'drawdown') => {
    let dstr = '', started = false;
    for (const p of points) {
      const v = p[key];
      if (v == null) { started = false; continue; }
      dstr += `${started ? 'L' : 'M'}${xOf(p.date).toFixed(1)},${yOf(v).toFixed(1)} `;
      started = true;
    }
    return dstr.trim();
  };

  const grid = [1, RAMP_HI, 0.5, RAMP_LO, 0];

  const onMove = (e: ReactMouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    if (rect.width === 0) return;
    const frac = (e.clientX - rect.left) / rect.width;
    const tf = Math.max(0, Math.min(1, (frac - PAD_X / CHART_W) / (INNER_W / CHART_W)));
    const target = minMs + tf * (maxMs - minMs || 1);
    let bi = 0, bd = Infinity;
    for (let i = 0; i < times.length; i++) {
      const dd = Math.abs(times[i] - target);
      if (dd < bd) { bd = dd; bi = i; }
    }
    setHoverDate(points[bi].date);
  };

  // Snap the shared hovered date to the nearest health point, so hovering
  // EITHER chart lights up this one's guide at the aligned date.
  const hp = (() => {
    if (!hoverDate) return undefined;
    const t = Date.parse(hoverDate);
    let best: HealthPoint | undefined, bd = Infinity;
    for (const p of points) {
      const dd = Math.abs(Date.parse(p.date) - t);
      if (dd < bd) { bd = dd; best = p; }
    }
    return best;
  })();
  const guideLeft = hp ? xOf(hp.date) / CHART_W : 0;

  return (
    <div>
      <div className="flex gap-1.5">
        <div className="flex flex-col justify-between text-[9px] font-mono text-fg-faint w-6 text-right shrink-0 h-40 py-0.5">
          <span>1.0</span><span>{RAMP_HI}</span><span>0.5</span><span>{RAMP_LO}</span><span>0</span>
        </div>
        <div className="relative flex-1">
          <svg
            viewBox={`0 0 ${CHART_W} ${H}`}
            preserveAspectRatio="none"
            className="w-full h-40"
            onMouseMove={onMove}
            onMouseLeave={() => setHoverDate(null)}
          >
            <rect
              x={PAD_X} y={yOf(RAMP_HI)} width={INNER_W} height={yOf(RAMP_LO) - yOf(RAMP_HI)}
              fill="var(--color-accent-500)" opacity={0.06}
            />
            {grid.map((g) => {
              const major = g === RAMP_LO || g === RAMP_HI;
              return (
                <line
                  key={g} x1={PAD_X} x2={CHART_W - PAD_X} y1={yOf(g)} y2={yOf(g)}
                  stroke="var(--color-neutral-800)" strokeWidth={major ? 1 : 0.5}
                  opacity={major ? 0.45 : 0.18} strokeDasharray={major ? '3 2' : undefined}
                  vectorEffect="non-scaling-stroke"
                />
              );
            })}
            <path d={linePath('exposure')} fill="none" stroke={EXPOSURE_COLOR} strokeWidth={1.4} vectorEffect="non-scaling-stroke" />
            {showComponents && (
              <>
                <path d={linePath('trend')} fill="none" stroke={TREND_COLOR} strokeWidth={1} opacity={0.8} vectorEffect="non-scaling-stroke" />
                <path d={linePath('momentum')} fill="none" stroke={MOM_COLOR} strokeWidth={1} opacity={0.8} vectorEffect="non-scaling-stroke" />
                <path d={linePath('drawdown')} fill="none" stroke={DD_COLOR} strokeWidth={1} opacity={0.8} vectorEffect="non-scaling-stroke" />
              </>
            )}
            <path d={linePath('health')} fill="none" stroke={HEALTH_COLOR} strokeWidth={1.6} vectorEffect="non-scaling-stroke" />
          </svg>
          {hp && (
            <>
              <div className="absolute top-0 bottom-0 w-px pointer-events-none" style={{ left: `${guideLeft * 100}%`, background: 'var(--color-neutral-700)' }} />
              <div className="absolute w-1.5 h-1.5 rounded-full -translate-x-1/2 -translate-y-1/2 pointer-events-none ring-1 ring-card"
                style={{ left: `${guideLeft * 100}%`, top: `${(yOf(hp.health) / H) * 100}%`, background: HEALTH_COLOR }} />
              <div className="absolute w-1.5 h-1.5 rounded-full -translate-x-1/2 -translate-y-1/2 pointer-events-none ring-1 ring-card"
                style={{ left: `${guideLeft * 100}%`, top: `${(yOf(hp.exposure) / H) * 100}%`, background: EXPOSURE_COLOR }} />
              <div className="absolute top-1 pointer-events-none whitespace-nowrap rounded-md bg-popover border border-neutral-700 px-2 py-1 text-[10px] shadow-xl z-30"
                style={{ left: `${guideLeft * 100}%`, transform: tipTransform(guideLeft) }}>
                <div className="font-mono text-fg-soft mb-0.5">{hp.date}</div>
                <div className="font-mono flex items-center gap-1">
                  <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: HEALTH_COLOR }} />
                  <span className="text-fg-muted">health</span><span className="text-fg-soft">{hp.health.toFixed(2)}</span>
                </div>
                <div className="font-mono flex items-center gap-1">
                  <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: EXPOSURE_COLOR }} />
                  <span className="text-fg-muted">exposure</span><span className="text-fg-soft">{(hp.exposure * 100).toFixed(0)}%</span>
                </div>
                {showComponents && hp.trend != null && (
                  <div className="font-mono flex items-center gap-1">
                    <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: TREND_COLOR }} />
                    <span className="text-fg-muted">trend</span><span className="text-fg-soft">{hp.trend.toFixed(2)}</span>
                  </div>
                )}
                {showComponents && hp.momentum != null && (
                  <div className="font-mono flex items-center gap-1">
                    <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: MOM_COLOR }} />
                    <span className="text-fg-muted">momentum</span><span className="text-fg-soft">{hp.momentum.toFixed(2)}</span>
                  </div>
                )}
                {showComponents && hp.drawdown != null && (
                  <div className="font-mono flex items-center gap-1">
                    <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: DD_COLOR }} />
                    <span className="text-fg-muted">drawdown</span><span className="text-fg-soft">{hp.drawdown.toFixed(2)}</span>
                  </div>
                )}
              </div>
            </>
          )}
        </div>
      </div>
      <div className="flex justify-between text-[9px] font-mono text-fg-faint mt-0.5 pl-[1.875rem]">
        <span>{fmtDate(minMs)}</span><span>{fmtDate(maxMs)}</span>
      </div>
    </div>
  );
}

function BenchmarkChart({
  name, points, xOf, minMs, span, hoverDate, setHoverDate,
}: {
  name: string;
  points: BenchPoint[];
  xOf: (date: string) => number;
  minMs: number;
  span: number;
  hoverDate: string | null;
  setHoverDate: (d: string | null) => void;
}) {
  const H = 70, padTop = 4, padBot = 4;
  const innerH = H - padTop - padBot;

  // LOG y-scale: equal % moves → equal vertical moves, so past crises (when
  // the index level was far lower) read as clearly as recent ones. A linear
  // scale would flatten a 2008/2020 −40% next to today's higher absolute
  // level. Prices are positive; guard defensively all the same.
  const prices = points.map((p) => p.price).filter((v) => v > 0);
  const pMin = Math.min(...prices), pMax = Math.max(...prices);
  const lMin = Math.log(pMin), lMax = Math.log(pMax);
  const lSpan = lMax - lMin || 1;
  const yOf = (price: number) =>
    padTop + (1 - (Math.log(Math.max(price, 1e-9)) - lMin) / lSpan) * innerH;

  const times = points.map((p) => Date.parse(p.date));
  const linePath = (() => {
    let dstr = '', started = false;
    for (const p of points) {
      dstr += `${started ? 'L' : 'M'}${xOf(p.date).toFixed(1)},${yOf(p.price).toFixed(1)} `;
      started = true;
    }
    return dstr.trim();
  })();

  const onMove = (e: ReactMouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    if (rect.width === 0) return;
    const frac = (e.clientX - rect.left) / rect.width;
    const tf = Math.max(0, Math.min(1, (frac - PAD_X / CHART_W) / (INNER_W / CHART_W)));
    const target = minMs + tf * span;
    let bi = 0, bd = Infinity;
    for (let i = 0; i < times.length; i++) {
      const dd = Math.abs(times[i] - target);
      if (dd < bd) { bd = dd; bi = i; }
    }
    setHoverDate(points[bi].date);
  };

  // Snap the shared hovered date to the nearest benchmark point.
  const hp = (() => {
    if (!hoverDate) return undefined;
    const t = Date.parse(hoverDate);
    let best: BenchPoint | undefined, bd = Infinity;
    for (const p of points) {
      const dd = Math.abs(Date.parse(p.date) - t);
      if (dd < bd) { bd = dd; best = p; }
    }
    return best;
  })();
  const guideLeft = hp ? xOf(hp.date) / CHART_W : 0;

  return (
    <div>
      <div className="flex gap-1.5">
        <div className="flex flex-col justify-between text-[9px] font-mono text-fg-faint w-6 text-right shrink-0 h-32 py-0.5">
          <span>{pMax.toFixed(0)}</span><span>{pMin.toFixed(0)}</span>
        </div>
        <div className="relative flex-1">
          <svg
            viewBox={`0 0 ${CHART_W} ${H}`}
            preserveAspectRatio="none"
            className="w-full h-32"
            onMouseMove={onMove}
            onMouseLeave={() => setHoverDate(null)}
          >
            <path d={linePath} fill="none" stroke={BENCH_COLOR} strokeWidth={1.4} vectorEffect="non-scaling-stroke" />
          </svg>
          {hp && (
            <>
              <div className="absolute top-0 bottom-0 w-px pointer-events-none" style={{ left: `${guideLeft * 100}%`, background: 'var(--color-neutral-700)' }} />
              <div className="absolute w-1.5 h-1.5 rounded-full -translate-x-1/2 -translate-y-1/2 pointer-events-none ring-1 ring-card"
                style={{ left: `${guideLeft * 100}%`, top: `${(yOf(hp.price) / H) * 100}%`, background: BENCH_COLOR }} />
              <div className="absolute top-1 pointer-events-none whitespace-nowrap rounded-md bg-popover border border-neutral-700 px-2 py-1 text-[10px] shadow-xl z-30"
                style={{ left: `${guideLeft * 100}%`, transform: tipTransform(guideLeft) }}>
                <div className="font-mono text-fg-soft mb-0.5">{hp.date}</div>
                <div className="font-mono flex items-center gap-1">
                  <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: BENCH_COLOR }} />
                  <span className="text-fg-muted">{name}</span><span className="text-fg-soft">{hp.price.toFixed(2)}</span>
                </div>
                <div className="font-mono">
                  <span className="text-fg-muted">from peak </span>
                  <span className={hp.ddPct < 0 ? 'text-neg-400' : 'text-fg-soft'}>{hp.ddPct.toFixed(1)}%</span>
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

function RsiChart({
  points, xOf, hoverDate, setHoverDate,
}: {
  points: { date: string; simple: number | null; wilder: number | null }[];
  xOf: (date: string) => number;
  hoverDate: string | null;
  setHoverDate: (d: string | null) => void;
}) {
  const H = 70, padTop = 4, padBot = 4;
  const innerH = H - padTop - padBot;
  const yOf = (rsi: number) => padTop + (1 - rsi / 100) * innerH; // 0..100, 100 top

  const times = points.map((p) => Date.parse(p.date));
  const minMs = Math.min(...times), maxMs = Math.max(...times);

  const linePath = (key: 'simple' | 'wilder') => {
    let dstr = '', started = false;
    for (const p of points) {
      const v = p[key];
      if (v == null) { started = false; continue; }
      dstr += `${started ? 'L' : 'M'}${xOf(p.date).toFixed(1)},${yOf(v).toFixed(1)} `;
      started = true;
    }
    return dstr.trim();
  };

  const onMove = (e: ReactMouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    if (rect.width === 0) return;
    const frac = (e.clientX - rect.left) / rect.width;
    const tf = Math.max(0, Math.min(1, (frac - PAD_X / CHART_W) / (INNER_W / CHART_W)));
    const target = minMs + tf * (maxMs - minMs || 1);
    let bi = 0, bd = Infinity;
    for (let i = 0; i < times.length; i++) {
      const dd = Math.abs(times[i] - target);
      if (dd < bd) { bd = dd; bi = i; }
    }
    setHoverDate(points[bi].date);
  };

  const hp = (() => {
    if (!hoverDate) return undefined;
    const t = Date.parse(hoverDate);
    let best: { date: string; simple: number | null; wilder: number | null } | undefined, bd = Infinity;
    for (const p of points) {
      const dd = Math.abs(Date.parse(p.date) - t);
      if (dd < bd) { bd = dd; best = p; }
    }
    return best;
  })();
  const guideLeft = hp ? xOf(hp.date) / CHART_W : 0;
  const rsiLabel = (v: number) => (v >= 70 ? 'overbought' : v <= 30 ? 'oversold' : 'neutral');

  return (
    <div className="flex gap-1.5">
      <div className="flex flex-col justify-between text-[9px] font-mono text-fg-faint w-6 text-right shrink-0 h-32 py-0.5">
        <span>100</span><span>70</span><span>30</span><span>0</span>
      </div>
      <div className="relative flex-1">
        <svg
          viewBox={`0 0 ${CHART_W} ${H}`}
          preserveAspectRatio="none"
          className="w-full h-32"
          onMouseMove={onMove}
          onMouseLeave={() => setHoverDate(null)}
        >
          {/* Overbought / oversold shading + 30/50/70 guides. */}
          <rect x={PAD_X} y={yOf(100)} width={INNER_W} height={yOf(70) - yOf(100)} fill="var(--color-neg-500)" opacity={0.04} />
          <rect x={PAD_X} y={yOf(30)} width={INNER_W} height={yOf(0) - yOf(30)} fill="var(--color-pos-500)" opacity={0.04} />
          {[70, 50, 30].map((g) => (
            <line
              key={g} x1={PAD_X} x2={CHART_W - PAD_X} y1={yOf(g)} y2={yOf(g)}
              stroke="var(--color-neutral-800)" strokeWidth={g === 50 ? 0.5 : 1}
              opacity={g === 50 ? 0.18 : 0.4} strokeDasharray={g === 50 ? undefined : '3 2'}
              vectorEffect="non-scaling-stroke"
            />
          ))}
          <path d={linePath('simple')} fill="none" stroke={RSI_SIMPLE_COLOR} strokeWidth={1.4} vectorEffect="non-scaling-stroke" />
          <path d={linePath('wilder')} fill="none" stroke={RSI_WILDER_COLOR} strokeWidth={1.4} vectorEffect="non-scaling-stroke" />
        </svg>
        {hp && (
          <>
            <div className="absolute top-0 bottom-0 w-px pointer-events-none" style={{ left: `${guideLeft * 100}%`, background: 'var(--color-neutral-700)' }} />
            {hp.simple != null && (
              <div className="absolute w-1.5 h-1.5 rounded-full -translate-x-1/2 -translate-y-1/2 pointer-events-none ring-1 ring-card"
                style={{ left: `${guideLeft * 100}%`, top: `${(yOf(hp.simple) / H) * 100}%`, background: RSI_SIMPLE_COLOR }} />
            )}
            {hp.wilder != null && (
              <div className="absolute w-1.5 h-1.5 rounded-full -translate-x-1/2 -translate-y-1/2 pointer-events-none ring-1 ring-card"
                style={{ left: `${guideLeft * 100}%`, top: `${(yOf(hp.wilder) / H) * 100}%`, background: RSI_WILDER_COLOR }} />
            )}
            <div className="absolute top-1 pointer-events-none whitespace-nowrap rounded-md bg-popover border border-neutral-700 px-2 py-1 text-[10px] shadow-xl z-30"
              style={{ left: `${guideLeft * 100}%`, transform: tipTransform(guideLeft) }}>
              <div className="font-mono text-fg-soft mb-0.5">{hp.date}</div>
              {hp.simple != null && (
                <div className="font-mono flex items-center gap-1">
                  <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: RSI_SIMPLE_COLOR }} />
                  <span className="text-fg-muted">simple</span><span className="text-fg-soft">{hp.simple.toFixed(1)}</span>
                  <span className="text-fg-faint">{rsiLabel(hp.simple)}</span>
                </div>
              )}
              {hp.wilder != null && (
                <div className="font-mono flex items-center gap-1">
                  <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: RSI_WILDER_COLOR }} />
                  <span className="text-fg-muted">wilder</span><span className="text-fg-soft">{hp.wilder.toFixed(1)}</span>
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function Legend({ color, label }: { color: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1.5 text-[11px]">
      <span className="inline-block w-2.5 h-2.5 rounded-full shrink-0" style={{ background: color }} />
      <span className="text-fg-soft">{label}</span>
    </span>
  );
}
