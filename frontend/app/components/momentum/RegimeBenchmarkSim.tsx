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
 * Market-timing simulation on the Regime Detector: take a benchmark
 * (default SP500), hold it ONLY during months the filter is at full (100%)
 * exposure, sit in cash otherwise, and compare against buy-and-hold. Shows
 * an equity curve (log), headline stats (total / CAGR / max-DD / % invested),
 * and a per-year table (each year's buy-hold vs filtered return + months
 * held). Per-month detail is on the curve's hover.
 *
 * Causal by construction: the exposure for a period was decided from health
 * measured strictly before it, then applied to that period's benchmark move.
 */

const EPS = 0.9999; // exposure ≥ this → "full exposure" → held that month

const BH_COLOR = 'var(--color-fg-muted)';
const FILT_COLOR = 'var(--color-accent-500)';
const W = 200, PADX = 2, INW = W - 2 * PADX;

type Period = { date: string; benchRet: number; held: boolean; filtRet: number };
type CurvePt = { date: string; bh: number; filt: number; benchRet: number; held: boolean };

const fmtPct = (v: number, s = true) => `${s && v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
const cls = (v: number) => (v > 0 ? 'text-pos-400' : v < 0 ? 'text-neg-400' : 'text-fg-soft');

export default function RegimeBenchmarkSim({
  result,
  defaultCollapsed = false,
}: {
  result: BacktestResult;
  defaultCollapsed?: boolean;
}) {
  const { data: benchmarks } = useBenchmarks();
  const options = useMemo<BenchmarkOption[]>(() => (benchmarks ?? []) as BenchmarkOption[], [benchmarks]);
  const [benchId, setBenchId] = useState<number | null>(null);
  const [benchPrices, setBenchPrices] = useState<BenchmarkPrice[]>([]);
  const [hover, setHover] = useState<{ leftFrac: number; pt: CurvePt } | null>(null);

  // Default to an SP500-looking benchmark when one exists.
  useEffect(() => {
    if (benchId != null || options.length === 0) return;
    const sp = options.find((o) => /s\s*&?\s*p\s*500|sp500|spx|gspc/i.test(`${o.ticker} ${o.name ?? ''}`));
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setBenchId((sp ?? options[0]).benchmark_id);
  }, [options, benchId]);

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

  const periods = useMemo<Period[]>(() => {
    const recs = (result.monthly_records ?? [])
      .filter((r) => r.market_health != null)
      .map((r) => ({ date: r.date.slice(0, 10), exposure: r.exposure_scale ?? 1.0 }))
      .sort((a, b) => a.date.localeCompare(b.date));
    const prices = [...benchPrices].sort((a, b) => a.target_date.localeCompare(b.target_date));
    if (recs.length < 2 || prices.length < 2) return [];

    // Last benchmark close on or before `date` (binary search, prices asc).
    const asof = (date: string): number | null => {
      let lo = 0, hi = prices.length - 1, res: number | null = null;
      while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (prices[mid].target_date.slice(0, 10) <= date) { res = prices[mid].price; lo = mid + 1; }
        else hi = mid - 1;
      }
      return res;
    };

    const out: Period[] = [];
    for (let i = 0; i < recs.length - 1; i++) {
      const p0 = asof(recs[i].date), p1 = asof(recs[i + 1].date);
      if (p0 == null || p1 == null || p0 <= 0) continue;
      const benchRet = (p1 / p0 - 1) * 100;
      const held = recs[i].exposure >= EPS;
      out.push({ date: recs[i].date, benchRet, held, filtRet: held ? benchRet : 0 });
    }
    return out;
  }, [result, benchPrices]);

  const curve = useMemo<CurvePt[]>(() => {
    let bh = 1, filt = 1;
    return periods.map((p) => {
      bh *= 1 + p.benchRet / 100;
      filt *= 1 + p.filtRet / 100;
      return { date: p.date, bh: (bh - 1) * 100, filt: (filt - 1) * 100, benchRet: p.benchRet, held: p.held };
    });
  }, [periods]);

  const years = useMemo(() => {
    const map = new Map<string, { bh: number; filt: number; held: number; n: number }>();
    for (const p of periods) {
      const y = p.date.slice(0, 4);
      const e = map.get(y) ?? { bh: 1, filt: 1, held: 0, n: 0 };
      e.bh *= 1 + p.benchRet / 100;
      e.filt *= 1 + p.filtRet / 100;
      e.held += p.held ? 1 : 0;
      e.n += 1;
      map.set(y, e);
    }
    return [...map.entries()].map(([year, e]) => ({
      year, bh: (e.bh - 1) * 100, filt: (e.filt - 1) * 100, held: e.held, n: e.n,
    }));
  }, [periods]);

  const stats = useMemo(() => {
    if (curve.length < 2) return null;
    const span = (Date.parse(curve[curve.length - 1].date) - Date.parse(curve[0].date)) / (365.25 * 864e5);
    const calc = (key: 'bh' | 'filt') => {
      const finalF = 1 + curve[curve.length - 1][key] / 100;
      let peak = 1, maxdd = 0;
      for (const pt of curve) {
        const f = 1 + pt[key] / 100;
        if (f > peak) peak = f;
        const dd = (f / peak - 1) * 100;
        if (dd < maxdd) maxdd = dd;
      }
      return { total: (finalF - 1) * 100, cagr: span > 0 ? (Math.pow(finalF, 1 / span) - 1) * 100 : 0, maxdd };
    };
    const heldPct = (periods.filter((p) => p.held).length / periods.length) * 100;
    return { bh: calc('bh'), filt: calc('filt'), heldPct };
  }, [curve, periods]);

  const benchName =
    options.find((o) => o.benchmark_id === benchId)?.ticker ??
    options.find((o) => o.benchmark_id === benchId)?.name ?? 'Benchmark';

  return (
    <CollapsibleCard
      title="Filter-timed benchmark vs buy & hold"
      defaultCollapsed={defaultCollapsed}
      rightSlot={
        <select
          value={benchId ?? ''}
          onClick={(e) => e.stopPropagation()}
          onChange={(e) => setBenchId(e.target.value ? Number(e.target.value) : null)}
          className="bg-page border border-neutral-700 rounded-lg px-2 py-0.5 text-[11px] text-fg focus:border-accent-500 outline-none"
        >
          <option value="">(pick benchmark)</option>
          {options.map((o) => (
            <option key={o.benchmark_id} value={o.benchmark_id}>{o.ticker} — {o.name}</option>
          ))}
        </select>
      }
      bodyClassName="px-5 py-4 space-y-4"
    >
      <p className="text-[11px] text-fg-faint">
        Hold <span className="text-fg-soft">{benchName}</span> only in months the filter is at 100% exposure; cash otherwise. Equity curve is log-scaled so early moves read fairly. Per-month return is on hover; per-year below.
      </p>

      {benchId == null ? (
        <div className="text-[11px] text-fg-subtle">Pick a benchmark (e.g. SP500) to run the simulation.</div>
      ) : curve.length < 2 ? (
        <div className="text-[11px] text-fg-subtle">No overlapping {benchName} prices in this window.</div>
      ) : (
        <>
          {stats && (
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              <StatBox label={`Total · ${benchName}`} value={fmtPct(stats.bh.total)} c={cls(stats.bh.total)} />
              <StatBox label="Total · filtered" value={fmtPct(stats.filt.total)} c={cls(stats.filt.total)} />
              <StatBox label="Max DD · buy&hold" value={fmtPct(stats.bh.maxdd, false)} c="text-neg-400" />
              <StatBox label="Max DD · filtered" value={fmtPct(stats.filt.maxdd, false)} c="text-neg-400" />
              <StatBox label={`CAGR · ${benchName}`} value={fmtPct(stats.bh.cagr)} c={cls(stats.bh.cagr)} />
              <StatBox label="CAGR · filtered" value={fmtPct(stats.filt.cagr)} c={cls(stats.filt.cagr)} />
              <StatBox label="% months invested" value={`${stats.heldPct.toFixed(0)}%`} c="text-fg-soft" />
              <StatBox label="Months out (cash)" value={String(periods.filter((p) => !p.held).length)} c="text-fg-soft" />
            </div>
          )}

          <EquityCurve curve={curve} hover={hover} setHover={setHover} benchName={benchName} />
          <div className="flex flex-wrap justify-center gap-x-6 gap-y-1">
            <Legend color={BH_COLOR} label={`${benchName} buy & hold`} />
            <Legend color={FILT_COLOR} label="Filtered (100%-exposure months only)" />
          </div>

          {/* Per-year returns. */}
          <div className="overflow-x-auto">
            <table className="w-full text-[12px]">
              <thead>
                <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                  <th className="text-left py-1.5 font-medium">Year</th>
                  <th className="text-right py-1.5 font-medium">{benchName}</th>
                  <th className="text-right py-1.5 font-medium">Filtered</th>
                  <th className="text-right py-1.5 font-medium">Δ</th>
                  <th className="text-right py-1.5 font-medium">Held</th>
                </tr>
              </thead>
              <tbody>
                {years.map((y) => (
                  <tr key={y.year} className="border-b border-neutral-800/20">
                    <td className="py-1.5 font-mono text-fg-soft">{y.year}</td>
                    <td className={`py-1.5 text-right font-mono ${cls(y.bh)}`}>{fmtPct(y.bh)}</td>
                    <td className={`py-1.5 text-right font-mono ${cls(y.filt)}`}>{fmtPct(y.filt)}</td>
                    <td className={`py-1.5 text-right font-mono ${cls(y.filt - y.bh)}`}>{fmtPct(y.filt - y.bh)}</td>
                    <td className="py-1.5 text-right font-mono text-fg-faint">{y.held}/{y.n}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </CollapsibleCard>
  );
}

function EquityCurve({
  curve, hover, setHover, benchName,
}: {
  curve: CurvePt[];
  hover: { leftFrac: number; pt: CurvePt } | null;
  setHover: (h: { leftFrac: number; pt: CurvePt } | null) => void;
  benchName: string;
}) {
  const H = 110, padTop = 4, padBot = 4;
  const innerH = H - padTop - padBot;

  const times = curve.map((p) => Date.parse(p.date));
  const minMs = Math.min(...times), maxMs = Math.max(...times), span = maxMs - minMs || 1;
  const xOf = (date: string) => PADX + ((Date.parse(date) - minMs) / span) * INW;

  // Log y over both series' growth factors.
  const fs = curve.flatMap((p) => [1 + p.bh / 100, 1 + p.filt / 100]).filter((v) => v > 0);
  const lMin = Math.log(Math.min(...fs)), lMax = Math.log(Math.max(...fs)), lSpan = lMax - lMin || 1;
  const yOf = (pct: number) => padTop + (1 - (Math.log(Math.max(1 + pct / 100, 1e-9)) - lMin) / lSpan) * innerH;

  const path = (key: 'bh' | 'filt') => {
    let d = '', started = false;
    for (const p of curve) {
      d += `${started ? 'L' : 'M'}${xOf(p.date).toFixed(1)},${yOf(p[key]).toFixed(1)} `;
      started = true;
    }
    return d.trim();
  };

  const onMove = (e: ReactMouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    if (rect.width === 0) return;
    const frac = (e.clientX - rect.left) / rect.width;
    const tf = Math.max(0, Math.min(1, (frac - PADX / W) / (INW / W)));
    const target = minMs + tf * span;
    let bi = 0, bd = Infinity;
    for (let i = 0; i < times.length; i++) {
      const dd = Math.abs(times[i] - target);
      if (dd < bd) { bd = dd; bi = i; }
    }
    setHover({ leftFrac: xOf(curve[bi].date) / W, pt: curve[bi] });
  };

  const tipTx = hover ? `translateX(${(-hover.leftFrac * 100).toFixed(1)}%)` : '';

  return (
    <div className="flex gap-1.5">
      <div className="flex flex-col justify-between text-[9px] font-mono text-fg-faint w-8 text-right shrink-0 h-48 py-0.5">
        <span>{fmtPct(curve[curve.length - 1].bh, false)}</span>
        <span>0%</span>
      </div>
      <div className="relative flex-1">
        <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="w-full h-48" onMouseMove={onMove} onMouseLeave={() => setHover(null)}>
          {/* break-even (0%) line */}
          <line x1={PADX} x2={W - PADX} y1={yOf(0)} y2={yOf(0)} stroke="var(--color-neutral-800)" strokeWidth={0.75} strokeDasharray="3 2" opacity={0.4} vectorEffect="non-scaling-stroke" />
          {/* cash months: faint marks under the filtered line where we sat out */}
          {curve.map((p, i) => (!p.held ? (
            <line key={i} x1={xOf(p.date)} x2={xOf(p.date)} y1={H - padBot - 3} y2={H - padBot} stroke="var(--color-warn-500)" strokeWidth={1} opacity={0.5} vectorEffect="non-scaling-stroke" />
          ) : null))}
          <path d={path('bh')} fill="none" stroke={BH_COLOR} strokeWidth={1.3} vectorEffect="non-scaling-stroke" />
          <path d={path('filt')} fill="none" stroke={FILT_COLOR} strokeWidth={1.6} vectorEffect="non-scaling-stroke" />
        </svg>
        {hover && (
          <>
            <div className="absolute top-0 bottom-0 w-px pointer-events-none" style={{ left: `${hover.leftFrac * 100}%`, background: 'var(--color-neutral-700)' }} />
            <div className="absolute top-1 pointer-events-none whitespace-nowrap rounded-md bg-popover border border-neutral-700 px-2 py-1 text-[10px] shadow-xl z-30"
              style={{ left: `${hover.leftFrac * 100}%`, transform: tipTx }}>
              <div className="font-mono text-fg-soft mb-0.5">{hover.pt.date} · {hover.pt.held ? 'held' : 'cash'}</div>
              <div className="font-mono">month <span className={cls(hover.pt.benchRet)}>{fmtPct(hover.pt.benchRet)}</span></div>
              <div className="font-mono flex items-center gap-1">
                <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: BH_COLOR }} />
                <span className="text-fg-muted">{benchName}</span><span className={cls(hover.pt.bh)}>{fmtPct(hover.pt.bh)}</span>
              </div>
              <div className="font-mono flex items-center gap-1">
                <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: FILT_COLOR }} />
                <span className="text-fg-muted">filtered</span><span className={cls(hover.pt.filt)}>{fmtPct(hover.pt.filt)}</span>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function StatBox({ label, value, c }: { label: string; value: string; c: string }) {
  return (
    <div className="rounded-lg border border-neutral-800/40 bg-page/40 px-3 py-2">
      <div className="text-[10px] text-fg-faint">{label}</div>
      <div className={`font-mono text-sm ${c}`}>{value}</div>
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
