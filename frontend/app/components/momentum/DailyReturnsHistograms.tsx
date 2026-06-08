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
 * Daily-return distributions for the strategy, the equal-weight universe, and
 * a selectable benchmark — overlaid as step outlines sharing one From/To
 * date-range slider (the same window applies to all three). Pure SVG (no
 * recharts): fixed 0.5%-wide bins on a clamped ±8% range, density-normalised
 * (% of days) so unequal observation counts compare fairly, with a dashed
 * vertical MEAN marker per series so a μ difference between strategy and
 * universe is visible (it's invisible from the bars alone — the daily-mean
 * gap is tiny next to the daily spread).
 */
type DailyRet = { date: string; ret: number };

const BIN_W = 0.5;                        // bin width, %
const X_MAX = 8;                          // clamp range, ±%
const N_EDGES = (2 * X_MAX) / BIN_W + 1;  // 33 edges → 32 bins
const ROLL = 21;                          // default rolling-correlation window (≈1 trading month)
const ROLL_OPTIONS = [10, 21, 42, 63, 126, 252];
const rollLabel = (d: number) =>
  ({ 21: '≈1mo', 42: '≈2mo', 63: '≈3mo', 126: '≈6mo', 252: '≈1yr' } as Record<number, string>)[d] ?? `${d}d`;
const UNI_COLOR = 'var(--color-fg-muted)';
const BENCH_COLOR = 'var(--color-warn-500)';

/** Daily % returns from a cumulative-return curve. Dedupes repeated dates
 * (the curve repeats period-boundary dates — exit of one period == entry of
 * the next — which would otherwise inject spurious ~0% boundary returns). */
function fromCumulative(recs: { date: string; cumulative_return_pct: number }[]): DailyRet[] {
  const seen = new Map<string, number>();
  const clean: { date: string; cum: number }[] = [];
  for (const r of recs) {
    const d = r.date.slice(0, 10);
    const idx = seen.get(d);
    if (idx === undefined) { seen.set(d, clean.length); clean.push({ date: d, cum: r.cumulative_return_pct }); }
    else { clean[idx].cum = r.cumulative_return_pct; }
  }
  const out: DailyRet[] = [];
  for (let i = 1; i < clean.length; i++) {
    const f0 = 1 + clean[i - 1].cum / 100;
    const f1 = 1 + clean[i].cum / 100;
    if (f0 > 0) out.push({ date: clean[i].date, ret: (f1 / f0 - 1) * 100 });
  }
  return out;
}

/** Daily % returns from a benchmark price series. */
function fromPrices(prices: BenchmarkPrice[]): DailyRet[] {
  const sorted = [...prices].sort((a, b) => a.target_date.localeCompare(b.target_date));
  const out: DailyRet[] = [];
  for (let i = 1; i < sorted.length; i++) {
    const p0 = sorted[i - 1].price;
    const p1 = sorted[i].price;
    if (p0 > 0) out.push({ date: sorted[i].target_date.slice(0, 10), ret: (p1 / p0 - 1) * 100 });
  }
  return out;
}

const inWindow = (rows: DailyRet[], from: string, to: string) =>
  rows.filter((r) => r.date >= from && r.date <= to);

function binCounts(values: number[], edges: number[]): number[] {
  const counts = new Array(edges.length - 1).fill(0);
  const lo = edges[0];
  const hi = edges[edges.length - 1];
  const w = (hi - lo) / (edges.length - 1);
  if (w <= 0) return counts;
  for (const v of values) {
    let b = Math.floor((v - lo) / w);
    if (b < 0) b = 0;
    if (b > counts.length - 1) b = counts.length - 1;
    counts[b] += 1;
  }
  return counts;
}

function stats(values: number[]): { n: number; mean: number; std: number } {
  const n = values.length;
  if (n === 0) return { n: 0, mean: 0, std: 0 };
  const mean = values.reduce((a, b) => a + b, 0) / n;
  const std = Math.sqrt(values.reduce((a, b) => a + (b - mean) ** 2, 0) / n);
  return { n, mean, std };
}

type Tail = {
  skew: number; kurt: number;     // skewness, excess kurtosis
  var95: number; cvar95: number;  // 95% Value-at-Risk + Conditional VaR (left tail, %)
  best: number; worst: number;    // best / worst single day, %
};

/** Higher-moment + tail risk of a daily-return series. */
function tailStats(values: number[]): Tail | null {
  const n = values.length;
  if (n < 3) return null;
  const mean = values.reduce((a, b) => a + b, 0) / n;
  const sd = Math.sqrt(values.reduce((a, b) => a + (b - mean) ** 2, 0) / n);
  let skew = 0, kurt = 0;
  if (sd > 0) {
    for (const v of values) { const z = (v - mean) / sd; skew += z ** 3; kurt += z ** 4; }
    skew /= n; kurt = kurt / n - 3; // excess kurtosis (0 = normal)
  }
  const sorted = [...values].sort((a, b) => a - b);
  const var95 = sorted[Math.max(0, Math.floor(0.05 * (n - 1)))]; // 5th-percentile daily return
  const tail = sorted.filter((v) => v <= var95);
  const cvar95 = tail.length ? tail.reduce((a, b) => a + b, 0) / tail.length : var95;
  return { skew, kurt, var95, cvar95, best: sorted[n - 1], worst: sorted[0] };
}

type Corr = {
  points: [number, number][]; // [comparison ret (x), strategy ret (y)]
  r: number | null;           // Pearson correlation
  beta: number | null;        // slope of strategy on comparison
  meanX: number; meanY: number;
  n: number;
};

/** Pair `base` (strategy) against `other` (universe/benchmark) on shared dates
 * and compute the daily-return correlation + regression slope (β). */
function pairAndStats(base: DailyRet[], other: DailyRet[]): Corr {
  const byDate = new Map(base.map((d) => [d.date, d.ret]));
  const points: [number, number][] = [];
  for (const o of other) {
    const y = byDate.get(o.date);
    if (y !== undefined) points.push([o.ret, y]);
  }
  const nn = points.length;
  if (nn < 2) return { points, r: null, beta: null, meanX: 0, meanY: 0, n: nn };
  const mx = points.reduce((a, p) => a + p[0], 0) / nn;
  const my = points.reduce((a, p) => a + p[1], 0) / nn;
  let sxy = 0, sxx = 0, syy = 0;
  for (const [x, y] of points) {
    const dx = x - mx, dy = y - my;
    sxy += dx * dy; sxx += dx * dx; syy += dy * dy;
  }
  return {
    points,
    r: sxx > 0 && syy > 0 ? sxy / Math.sqrt(sxx * syy) : null,
    beta: sxx > 0 ? sxy / sxx : null,
    meanX: mx, meanY: my, n: nn,
  };
}

type RollPoint = { date: string; r: number | null };

/** Trailing-window Pearson correlation of `base` (strategy) vs `other`
 * (universe/benchmark), one point per shared trading day once `window`
 * paired observations are available. */
function rollingCorr(base: DailyRet[], other: DailyRet[], window: number): RollPoint[] {
  const byDate = new Map(base.map((d) => [d.date, d.ret]));
  const pairs: { date: string; x: number; y: number }[] = [];
  for (const o of other) {
    const y = byDate.get(o.date);
    if (y !== undefined) pairs.push({ date: o.date, x: o.ret, y });
  }
  pairs.sort((a, b) => a.date.localeCompare(b.date));
  const out: RollPoint[] = [];
  for (let i = window - 1; i < pairs.length; i++) {
    const s = i - window + 1;
    let sx = 0, sy = 0;
    for (let j = s; j <= i; j++) { sx += pairs[j].x; sy += pairs[j].y; }
    const mx = sx / window, my = sy / window;
    let sxy = 0, sxx = 0, syy = 0;
    for (let j = s; j <= i; j++) {
      const dx = pairs[j].x - mx, dy = pairs[j].y - my;
      sxy += dx * dy; sxx += dx * dx; syy += dy * dy;
    }
    out.push({ date: pairs[i].date, r: sxx > 0 && syy > 0 ? sxy / Math.sqrt(sxx * syy) : null });
  }
  return out;
}

export default function DailyReturnsHistograms({
  result,
  defaultCollapsed = false,
}: {
  result: BacktestResult;
  defaultCollapsed?: boolean;
}) {
  const { data: benchmarks } = useBenchmarks();
  const options = useMemo<BenchmarkOption[]>(() => (benchmarks ?? []) as BenchmarkOption[], [benchmarks]);

  // Master date axis = the strategy's daily curve (universe is aligned to it).
  const dateAxis = useMemo(
    () => (result.daily_records ?? []).map((d) => d.date.slice(0, 10)),
    [result],
  );
  const n = dateAxis.length;

  const [range, setRange] = useState<[number, number]>([0, Math.max(0, n - 1)]);
  // Reset the window to full whenever a new backtest loads.
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setRange([0, Math.max(0, n - 1)]);
  }, [n]);

  const [rollWindow, setRollWindow] = useState<number>(ROLL);

  const [benchId, setBenchId] = useState<number | null>(null);
  // Default to the first benchmark once options arrive.
  useEffect(() => {
    if (benchId == null && options.length > 0) {
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setBenchId(options[0].benchmark_id);
    }
  }, [options, benchId]);

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

  const startIdx = Math.min(range[0], Math.max(0, n - 1));
  const endIdx = Math.min(Math.max(range[1], startIdx), Math.max(0, n - 1));
  const fromDate = dateAxis[startIdx] ?? '';
  const toDate = dateAxis[endIdx] ?? '';

  const series = useMemo(() => {
    const strat = inWindow(fromCumulative(result.daily_records ?? []), fromDate, toDate);
    const uni = inWindow(fromCumulative(result.universe_daily_records ?? []), fromDate, toDate);
    const bench = inWindow(fromPrices(benchPrices), fromDate, toDate);
    // Fixed 0.5%-wide bins on a clamped ±X_MAX% range — stable/comparable
    // across windows and runs (outliers land in the edge bins). stats() below
    // uses the raw, unclamped returns so μ / σ stay exact.
    const edges = Array.from({ length: N_EDGES }, (_, i) => -X_MAX + i * BIN_W);
    const build = (rows: DailyRet[]) => {
      const vals = rows.map((r) => r.ret);
      return { counts: binCounts(vals, edges), ...stats(vals) };
    };
    return {
      edges,
      strat: build(strat),
      uni: build(uni),
      bench: build(bench),
      // Daily-return correlation of the strategy vs each comparison, paired
      // on shared trading days within the window — full-window r/β plus a
      // trailing-window rolling series.
      corrUni: pairAndStats(strat, uni),
      corrBench: pairAndStats(strat, bench),
      rollUni: rollingCorr(strat, uni, rollWindow),
      rollBench: rollingCorr(strat, bench, rollWindow),
      stratTail: tailStats(strat.map((r) => r.ret)),
    };
  }, [result, benchPrices, fromDate, toDate, rollWindow]);

  const benchName = options.find((o) => o.benchmark_id === benchId)?.ticker
    ?? options.find((o) => o.benchmark_id === benchId)?.name
    ?? 'Benchmark';

  if (n < 2) {
    return (
      <CollapsibleCard title="Daily return distribution" defaultCollapsed={defaultCollapsed} bodyClassName="px-5 py-4">
        <div className="text-xs text-fg-subtle">No daily equity curve available for this run.</div>
      </CollapsibleCard>
    );
  }

  return (
    <CollapsibleCard
      title="Daily return distribution"
      defaultCollapsed={defaultCollapsed}
      rightSlot={<span className="text-[11px] font-mono text-fg-faint">{fromDate} → {toDate}</span>}
      bodyClassName="px-5 py-4 space-y-4"
    >
      {/* Controls: benchmark picker + From/To range sliders (one window for all). */}
      <div className="flex flex-wrap items-center gap-x-6 gap-y-3">
        <label className="flex items-center gap-2 text-xs text-fg-muted">
          Benchmark
          <select
            value={benchId ?? ''}
            onChange={(e) => setBenchId(e.target.value ? Number(e.target.value) : null)}
            className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
          >
            {options.length === 0 && <option value="">(none)</option>}
            {options.map((o) => (
              <option key={o.benchmark_id} value={o.benchmark_id}>{o.ticker} — {o.name}</option>
            ))}
          </select>
        </label>
        <div className="flex-1 min-w-[260px] space-y-1.5">
          <Slider label="From" value={startIdx} max={n - 1} date={fromDate}
            onChange={(v) => setRange(([, e]) => [Math.min(v, e), e])} />
          <Slider label="To" value={endIdx} max={n - 1} date={toDate}
            onChange={(v) => setRange(([s]) => [s, Math.max(v, s)])} />
        </div>
      </div>

      <Overlay
        edges={series.edges}
        plots={[
          { key: 'strat', label: 'Strategy', color: 'var(--color-accent-500)', d: series.strat },
          { key: 'uni', label: 'Universe (equal-weight)', color: 'var(--color-fg-muted)', d: series.uni },
          { key: 'bench', label: benchName, color: 'var(--color-warn-500)', d: series.bench },
        ]}
      />

      {/* Mean-difference readout — the question the dashed lines answer. */}
      {series.strat.n > 0 && series.uni.n > 0 && (
        <div className="text-[11px] text-fg-subtle">
          Δμ daily (strategy − universe):{' '}
          <span className={`font-mono font-medium ${series.strat.mean - series.uni.mean >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>
            {series.strat.mean - series.uni.mean >= 0 ? '+' : ''}{(series.strat.mean - series.uni.mean).toFixed(3)}%/day
          </span>
          {series.bench.n > 0 && (
            <>
              {'  ·  vs benchmark: '}
              <span className={`font-mono font-medium ${series.strat.mean - series.bench.mean >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>
                {series.strat.mean - series.bench.mean >= 0 ? '+' : ''}{(series.strat.mean - series.bench.mean).toFixed(3)}%/day
              </span>
            </>
          )}
        </div>
      )}
      {/* Strategy tail risk + shape — the moments the μ/σ above don't capture. */}
      {series.stratTail && (
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-fg-subtle">
          <span className="text-[10px] uppercase tracking-wide text-fg-faint">Strategy tails</span>
          <span title="Skewness of daily returns. Negative = a longer/​fatter left (loss) tail — typical of momentum.">
            skew <span className={`font-mono ${series.stratTail.skew >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>{series.stratTail.skew >= 0 ? '+' : ''}{series.stratTail.skew.toFixed(2)}</span>
          </span>
          <span title="Excess kurtosis (0 = normal). High = fat tails / more extreme days than a bell curve.">
            kurt <span className="font-mono text-fg-soft">{series.stratTail.kurt.toFixed(1)}</span>
          </span>
          <span title="95% Value-at-Risk: the daily loss the strategy exceeds on its worst ~5% of days.">
            VaR₉₅ <span className="font-mono text-neg-400">{series.stratTail.var95.toFixed(2)}%</span>
          </span>
          <span title="95% Conditional VaR (expected shortfall): the average daily return across that worst ~5% of days.">
            CVaR₉₅ <span className="font-mono text-neg-400">{series.stratTail.cvar95.toFixed(2)}%</span>
          </span>
          <span>best <span className="font-mono text-pos-400">{series.stratTail.best >= 0 ? '+' : ''}{series.stratTail.best.toFixed(2)}%</span></span>
          <span>worst <span className="font-mono text-neg-400">{series.stratTail.worst.toFixed(2)}%</span></span>
        </div>
      )}
      <p className="text-[10px] text-fg-faint">
        Daily % returns over the selected window — overlaid step outlines, density-normalised (% of days), fixed 0.5% bins clamped to ±{X_MAX}%. Solid line = distribution; dashed line = that series&apos; mean (μ). The gap between the dashed lines is the daily-return edge.
        {benchId == null && options.length > 0 ? ' Pick a benchmark to add its line.' : ''}
      </p>

      {/* Rolling correlation — how the strategy's daily-return co-movement
          with the universe / benchmark evolves over time (trailing ROLL-day
          window). The full-window r · β sit underneath as the summary. */}
      <div className="pt-3 border-t border-neutral-800/40">
        <div className="flex items-center justify-between gap-2 mb-2">
          <span className="text-[11px] uppercase tracking-wide text-fg-faint">Rolling correlation · {rollWindow}-day ({rollLabel(rollWindow)})</span>
          <label className="flex items-center gap-1.5 text-[11px] text-fg-muted">
            Window
            <select
              value={rollWindow}
              onChange={(e) => setRollWindow(Number(e.target.value))}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-0.5 text-[11px] text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
            >
              {ROLL_OPTIONS.map((d) => <option key={d} value={d}>{d}d ({rollLabel(d)})</option>)}
            </select>
          </label>
        </div>
        <RollingCorr uni={series.rollUni} bench={series.rollBench} hasBench={series.corrBench.n > 0} window={rollWindow} />
        <div className="flex flex-wrap justify-center gap-x-6 gap-y-1 mt-2">
          <CorrLegend color={UNI_COLOR} label="vs Universe (full window)" c={series.corrUni} />
          {series.corrBench.n > 0 && <CorrLegend color={BENCH_COLOR} label={`vs ${benchName} (full window)`} c={series.corrBench} />}
        </div>
      </div>
    </CollapsibleCard>
  );
}

function RollingCorr({ uni, bench, hasBench, window }: { uni: RollPoint[]; bench: RollPoint[]; hasBench: boolean; window: number }) {
  const [hover, setHover] = useState<{ leftFrac: number; date: string; uniR: number | null; benchR: number | null } | null>(null);
  const valid = [...uni, ...bench].filter((p) => p.r != null);
  if (valid.length < 2) {
    return <div className="text-[11px] text-fg-subtle">Not enough overlapping days for a {window}-day rolling correlation in this date range.</div>;
  }
  const W = 200, H = 70, padX = 2, padTop = 4, padBot = 4;
  const innerW = W - 2 * padX, innerH = H - padTop - padBot;
  const times = valid.map((p) => Date.parse(p.date));
  const minMs = Math.min(...times), maxMs = Math.max(...times);
  const span = maxMs - minMs || 1;
  const xOf = (d: string) => padX + ((Date.parse(d) - minMs) / span) * innerW;
  const yOf = (r: number) => padTop + (1 - (r + 1) / 2) * innerH; // r=+1 top, −1 bottom
  const linePath = (arr: RollPoint[]) => {
    let dstr = '', started = false;
    for (const p of arr) {
      if (p.r == null) { started = false; continue; }
      dstr += `${started ? 'L' : 'M'}${xOf(p.date).toFixed(1)},${yOf(p.r).toFixed(1)} `;
      started = true;
    }
    return dstr.trim();
  };
  const grid = [1, 0.5, 0, -0.5, -1];
  const fmtDate = (ms: number) => new Date(ms).toISOString().slice(0, 10);

  // Hover: snap the cursor to the nearest dated point and surface its values.
  const uniByDate = new Map(uni.map((p) => [p.date, p.r]));
  const benchByDate = new Map(bench.map((p) => [p.date, p.r]));
  const allDates = Array.from(new Set(valid.map((p) => p.date))).sort();
  const allTimes = allDates.map((d) => Date.parse(d));

  const onMove = (e: ReactMouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    if (rect.width === 0) return;
    const frac = (e.clientX - rect.left) / rect.width;
    const tf = Math.max(0, Math.min(1, (frac - padX / W) / (innerW / W)));
    const target = minMs + tf * span;
    let bi = 0, bd = Infinity;
    for (let i = 0; i < allTimes.length; i++) {
      const dd = Math.abs(allTimes[i] - target);
      if (dd < bd) { bd = dd; bi = i; }
    }
    const date = allDates[bi];
    setHover({ leftFrac: xOf(date) / W, date, uniR: uniByDate.get(date) ?? null, benchR: benchByDate.get(date) ?? null });
  };

  const tipTx = hover ? (hover.leftFrac < 0.12 ? '0' : hover.leftFrac > 0.88 ? '-100%' : '-50%') : '-50%';

  return (
    <div>
      <div className="flex gap-1.5">
        <div className="flex flex-col justify-between text-[9px] font-mono text-fg-faint w-5 text-right shrink-0 h-28 py-0.5">
          <span>+1</span><span>0</span><span>−1</span>
        </div>
        <div className="relative flex-1">
          <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="w-full h-28"
            onMouseMove={onMove} onMouseLeave={() => setHover(null)}>
            {grid.map((r) => {
              // +1 / 0 / −1 get clear lines; the ±0.5 helpers stay faint.
              const major = r === 0 || Math.abs(r) === 1;
              return (
                <line key={r} x1={padX} x2={W - padX} y1={yOf(r)} y2={yOf(r)}
                  stroke="var(--color-neutral-800)" strokeWidth={major ? 1 : 0.5}
                  opacity={major ? 0.6 : 0.18} vectorEffect="non-scaling-stroke" />
              );
            })}
            <path d={linePath(uni)} fill="none" stroke={UNI_COLOR} strokeWidth={1.4} vectorEffect="non-scaling-stroke" />
            {hasBench && <path d={linePath(bench)} fill="none" stroke={BENCH_COLOR} strokeWidth={1.4} vectorEffect="non-scaling-stroke" />}
          </svg>
          {hover && (
            <>
              {/* snap guide */}
              <div className="absolute top-0 bottom-0 w-px pointer-events-none" style={{ left: `${hover.leftFrac * 100}%`, background: 'var(--color-neutral-700)' }} />
              {hover.uniR != null && (
                <div className="absolute w-1.5 h-1.5 rounded-full -translate-x-1/2 -translate-y-1/2 pointer-events-none ring-1 ring-card"
                  style={{ left: `${hover.leftFrac * 100}%`, top: `${(yOf(hover.uniR) / H) * 100}%`, background: UNI_COLOR }} />
              )}
              {hasBench && hover.benchR != null && (
                <div className="absolute w-1.5 h-1.5 rounded-full -translate-x-1/2 -translate-y-1/2 pointer-events-none ring-1 ring-card"
                  style={{ left: `${hover.leftFrac * 100}%`, top: `${(yOf(hover.benchR) / H) * 100}%`, background: BENCH_COLOR }} />
              )}
              {/* tooltip */}
              <div className="absolute top-0 -mt-1 pointer-events-none whitespace-nowrap rounded-md bg-popover border border-neutral-700 px-2 py-1 text-[10px] shadow-xl z-10"
                style={{ left: `${hover.leftFrac * 100}%`, transform: `translateX(${tipTx}) translateY(-100%)` }}>
                <div className="font-mono text-fg-soft mb-0.5">{hover.date}</div>
                {hover.uniR != null && (
                  <div className="font-mono flex items-center gap-1">
                    <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: UNI_COLOR }} />
                    <span className="text-fg-muted">Universe</span><span className="text-fg-soft">r {hover.uniR.toFixed(2)}</span>
                  </div>
                )}
                {hasBench && hover.benchR != null && (
                  <div className="font-mono flex items-center gap-1">
                    <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: BENCH_COLOR }} />
                    <span className="text-fg-muted">Benchmark</span><span className="text-fg-soft">r {hover.benchR.toFixed(2)}</span>
                  </div>
                )}
              </div>
            </>
          )}
        </div>
      </div>
      <div className="flex justify-between text-[9px] font-mono text-fg-faint mt-0.5 pl-[1.625rem]">
        <span>{fmtDate(minMs)}</span><span>{fmtDate(maxMs)}</span>
      </div>
    </div>
  );
}

function CorrLegend({ color, label, c }: { color: string; label: string; c: Corr }) {
  return (
    <span className="inline-flex items-center gap-1.5 text-[11px]">
      <span className="inline-block w-2.5 h-2.5 rounded-full shrink-0" style={{ background: color }} />
      <span className="text-fg-soft">{label}</span>
      <span className="font-mono text-fg-faint">
        {c.r != null ? `r ${c.r.toFixed(2)} · β ${c.beta != null ? c.beta.toFixed(2) : '—'} · n ${c.n}` : '— no data'}
      </span>
    </span>
  );
}

function Slider({
  label, value, max, date, onChange,
}: { label: string; value: number; max: number; date: string; onChange: (v: number) => void }) {
  return (
    <div className="flex items-center gap-2 text-[11px] text-fg-subtle">
      <span className="w-8 shrink-0">{label}</span>
      <input
        type="range" min={0} max={max} value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="flex-1 h-1.5"
        style={{ accentColor: 'var(--color-accent-500)' }}
      />
      <span className="w-[5.5rem] shrink-0 text-right font-mono text-fg-soft">{date}</span>
    </div>
  );
}

type Plot = {
  key: string;
  label: string;
  color: string;
  d: { counts: number[]; n: number; mean: number; std: number };
};

function Overlay({ edges, plots }: { edges: number[]; plots: Plot[] }) {
  // viewBox units (stretched to fill width via preserveAspectRatio="none";
  // strokes use non-scaling-stroke so they stay crisp, and all text lives in
  // HTML below the svg so it isn't distorted by the non-uniform scale).
  const W = 200, H = 80, L = 2, R = 2, T = 4, B = 4;
  const plotW = W - L - R, plotH = H - T - B, baseY = T + plotH;
  const nBins = edges.length - 1;
  const bw = plotW / nBins;
  const xmax = edges[edges.length - 1];
  const xOf = (ret: number) => L + ((Math.max(-xmax, Math.min(xmax, ret)) + xmax) / (2 * xmax)) * plotW;

  // Shared y-scale = the tallest density bin across all drawn series.
  const maxD = Math.max(
    1e-9,
    ...plots.flatMap((p) => (p.d.n > 0 ? p.d.counts.map((c) => c / p.d.n) : [0])),
  );
  const build = (counts: number[], n: number) => {
    const pts: [number, number][] = [];
    for (let i = 0; i < nBins; i++) {
      const d = n > 0 ? counts[i] / n : 0;
      const y = baseY - (d / maxD) * plotH;
      pts.push([L + i * bw, y], [L + (i + 1) * bw, y]);
    }
    const stroke = pts.map((p, i) => `${i ? 'L' : 'M'}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(' ');
    const fill = `M${L},${baseY} ${pts.map((p) => `L${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(' ')} L${L + plotW},${baseY} Z`;
    return { stroke, fill };
  };
  const ticks = [-xmax, -xmax / 2, 0, xmax / 2, xmax];

  return (
    <div className="rounded-lg border border-neutral-800/40 bg-page/40 px-3 py-3">
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="w-full h-44">
        {/* faint shade over the down-day (negative) half */}
        <rect x={L} y={T} width={plotW / 2} height={plotH} fill="var(--color-neg-500)" opacity={0.04} />
        {ticks.map((t, i) => (
          <line key={`g${i}`} x1={xOf(t)} x2={xOf(t)} y1={T} y2={baseY}
            stroke="var(--color-neutral-800)" strokeWidth={t === 0 ? 1 : 0.5}
            opacity={t === 0 ? 0.55 : 0.25} vectorEffect="non-scaling-stroke" />
        ))}
        {plots.map((p) => {
          if (p.d.n === 0) return null;
          const path = build(p.d.counts, p.d.n);
          return (
            <g key={p.key}>
              <path d={path.fill} fill={p.color} opacity={0.09} />
              <path d={path.stroke} fill="none" stroke={p.color} strokeWidth={1.5} vectorEffect="non-scaling-stroke" />
              {/* dashed mean (μ) marker */}
              <line x1={xOf(p.d.mean)} x2={xOf(p.d.mean)} y1={T} y2={baseY}
                stroke={p.color} strokeWidth={1.25} strokeDasharray="3 2" vectorEffect="non-scaling-stroke" />
            </g>
          );
        })}
      </svg>
      {/* x-axis labels (HTML, evenly spaced to match the gridlines) */}
      <div className="flex justify-between text-[9px] font-mono text-fg-faint mt-0.5">
        {ticks.map((t, i) => <span key={i}>{t > 0 ? `+${t}` : t}%</span>)}
      </div>
      {/* legend with per-series μ / σ / n */}
      <div className="flex flex-wrap gap-x-5 gap-y-1 mt-2.5">
        {plots.map((p) => (
          <span key={p.key} className="inline-flex items-center gap-1.5 text-[11px]">
            <span className="inline-block w-2.5 h-2.5 rounded-full shrink-0" style={{ background: p.color }} />
            <span className="text-fg-soft">{p.label}</span>
            <span className="font-mono text-fg-faint">
              {p.d.n > 0 ? `μ ${p.d.mean >= 0 ? '+' : ''}${p.d.mean.toFixed(2)}% · σ ${p.d.std.toFixed(2)}% · n ${p.d.n}` : '— no data'}
            </span>
          </span>
        ))}
      </div>
    </div>
  );
}
