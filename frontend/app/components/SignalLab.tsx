'use client';

import { Fragment, useCallback, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { sectorLabel } from '../../lib/assetLabels';
import { REG, type RegKey } from './alphalab/regimeBands';
import { RANGES, type RangeId } from './alphalab/window';

const REGIME_ORDER: RegKey[] = ['bc', 'bt', 'rc', 'rt'];

type Cadence = 'month_end' | 'daily_asof';

type SignalRow = {
  signal: string; label: string; group: 'price' | 'volume' | 'trend';
  /** Which engine produced it. `daily_asof` = the signals /schedule actually trades.
   *  Load-bearing: `mom_12_1` and `daily.mom_12_1` are the same measure, but
   *  `vol_trend_3m` and `daily.vol_trend_3m` are NOT (spearman 0.58, opposite
   *  sign). Without the cadence rendered, those two rows are indistinguishable. */
  cadence?: Cadence;
  mean_ic: number; t_stat: number; p_value: number; hit_rate: number;
  quintile_spread: number | null; monotonicity: number | null; deciles: (number | null)[];
  months: number; significant: boolean;
  sector_ic?: Record<string, number>;
  regime_ic?: Partial<Record<RegKey, number>>;
  ic_series: { date: string; ic: number }[];
};
type Payload = {
  universe?: { name?: string; size?: number }; months?: number; from?: string; to?: string;
  sectors: string[]; regime_months?: Partial<Record<RegKey, number>>; signals: SignalRow[]; note?: string;
};

const pct = (x: number) => `${(x * 100).toFixed(1)}%`;
const num = (x: number | null, d = 3) => (x == null ? '—' : x.toFixed(d));

// IC → tinted cell (green positive / red negative, intensity by magnitude; IC≈±0.05 is strong).
function icStyle(ic: number | undefined): React.CSSProperties {
  if (ic == null) return {};
  const a = Math.min(0.85, Math.abs(ic) / 0.06 * 0.7 + 0.08);
  return { background: ic >= 0 ? `rgba(34,197,94,${a})` : `rgba(239,68,68,${a})` };
}

const GROUP_STYLE: Record<string, string> = {
  price: 'bg-accent-500/15 text-accent-300',
  volume: 'bg-warn-500/15 text-warn-300',
  trend: 'bg-pos-500/15 text-pos-300',
};

/** Two rows can carry near-identical labels across cadences ("volume trend 3m" vs
 *  "Volume Trend 3M") while measuring different things. Never render one without
 *  the other. */
function CadenceTag({ cadence }: { cadence?: Cadence }) {
  if (cadence !== 'daily_asof') return null;
  return (
    <span className="ml-1.5 text-[8px] uppercase tracking-wider px-1 py-0.5 rounded bg-pos-500/15 text-pos-300"
      title="Daily as-of cadence — the signal the live /schedule strategy trades (strict < cutoff, 30-day staleness guard)">
      live
    </span>
  );
}

export default function SignalLab() {
  const [universes, setUniverses] = useState<{ id: number; name: string; ticker_count: number }[]>([]);
  const [universeId, setUniverseId] = useState('');
  const [data, setData] = useState<Payload | null>(null);
  const [range, setRange] = useState<RangeId>('train');  // develop on train, validate on test
  const [loading, setLoading] = useState(false);
  const [elapsed, setElapsed] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState<string | null>(null);  // expanded signal (deciles)
  // Off by default: the daily battery loops per instrument, taking the 4,006-name
  // universe from ~31s to ~110s.
  const [includeDaily, setIncludeDaily] = useState(false);

  useEffect(() => {
    try { const r = localStorage.getItem('signalLab.range'); if (r === 'train' || r === 'test' || r === 'full') setRange(r); } catch { /* ignore */ }
    (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/asset-pipeline/universes`);
        const b = await r.json().catch(() => null);
        if (r.ok) setUniverses(b?.universes ?? []);
      } catch { /* ignore */ }
    })();
  }, []);
  useEffect(() => { try { localStorage.setItem('signalLab.range', range); } catch { /* ignore */ } }, [range]);

  useEffect(() => {
    if (!loading) { setElapsed(0); return; }
    const t0 = performance.now();
    const id = setInterval(() => setElapsed((performance.now() - t0) / 1000), 250);
    return () => clearInterval(id);
  }, [loading]);

  const load = useCallback(async (id: string, r: RangeId, daily: boolean) => {
    setLoading(true); setError(null); setData(null); setOpen(null);
    try {
      const w = RANGES[r];
      const p = new URLSearchParams({ universe_id: id });
      if (w.start) p.set('start', w.start);
      if (w.end) p.set('end', w.end);
      if (daily) p.set('include_daily', 'true');
      const resp = await apiFetch(`${API_URL}/api/asset-pipeline/signal-lab?${p}`);
      const b = await resp.json().catch(() => null);
      if (!resp.ok) setError(b?.detail ?? `HTTP ${resp.status}`);
      else if (!b?.signals?.length) setError(b?.note ?? 'No signals computed for this window.');
      else setData(b as Payload);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setLoading(false); }
  }, []);

  useEffect(() => {
    if (!universeId) { setData(null); setError(null); return; }
    void load(universeId, range, includeDaily);
  }, [universeId, range, includeDaily, load]);

  const sectors = data?.sectors ?? [];

  return (
    <div className="min-h-screen bg-page text-fg">
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">Signal Lab</h1>
        <p className="text-sm text-fg-subtle mt-1">
          Which price/volume signals predict next month’s cross-sectional returns — overall and per sector. Pure research; no portfolio.
        </p>
      </div>

      <div className="px-8 py-6 space-y-4">
        <div className="bg-card border border-neutral-800/40 rounded-xl p-4 space-y-3">
          {/* Train/test window — develop signals on training, validate on test. */}
          <div className="flex items-center gap-3 flex-wrap">
            <span className="text-xs font-semibold text-fg-strong uppercase tracking-wide">Window</span>
            <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5">
              {(Object.keys(RANGES) as RangeId[]).map((r) => (
                <button key={r} type="button" onClick={() => setRange(r)}
                  className={`text-[11px] px-2.5 py-1 rounded-md transition-colors ${
                    range === r ? 'bg-accent-600 text-white' : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/[0.04]'}`}>
                  {RANGES[r].label} <span className={range === r ? 'opacity-80' : 'opacity-50'}>{RANGES[r].span}</span>
                </button>
              ))}
            </div>
            <span className="text-[10px] text-fg-faint">develop signals on training · confirm they hold on test</span>
          </div>

          <div className="flex items-center gap-3 flex-wrap">
            <span className="text-xs font-semibold text-fg-strong uppercase tracking-wide">Universe</span>
            <select value={universeId} onChange={(e) => setUniverseId(e.target.value)}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-sm max-w-[280px] focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30">
              <option value="">Select a universe…</option>
              {universes.map((u) => <option key={u.id} value={u.id}>{u.name} ({u.ticker_count.toLocaleString()})</option>)}
            </select>
            <label className="flex items-center gap-1.5 text-[11px] text-fg-muted cursor-pointer select-none"
              title="Also score the daily as-of signals the live /schedule strategy trades. Much slower — it loops per instrument.">
              <input type="checkbox" checked={includeDaily} onChange={(e) => setIncludeDaily(e.target.checked)}
                className="accent-accent-600" />
              Live-strategy signals <span className="text-fg-faint">(slower)</span>
            </label>
            {loading && <span className="text-[11px] text-fg-faint">Computing… {elapsed.toFixed(1)}s</span>}
            {data && <span className="text-[11px] text-fg-faint">{RANGES[range].label} · {data.months} months · {data.from} → {data.to} · {data.universe?.size ?? 0} instruments</span>}
          </div>
          {loading && <div className="loading-bar h-0.5 w-full rounded-full" aria-hidden />}
          {error && <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>}
          {!data && !error && !loading && <p className="text-[11px] text-fg-subtle">Pick a universe to score its signals.</p>}
        </div>

        {/* Signal summary — ranked by |t-stat| */}
        {data && (
          <div className="bg-card border border-neutral-800/40 rounded-xl p-4 space-y-2">
            <div className="text-sm font-semibold text-fg-strong">Predictive power</div>
            <p className="text-[10px] text-fg-faint">
              IC = monthly rank correlation of the signal with next-month return · |t| ≥ 2 = significant · monotonicity = how cleanly deciles line up (1 = perfect) · click a row for its decile profile.
            </p>
            <div className="overflow-auto rounded-lg border border-neutral-800/40">
              <table className="w-full text-xs">
                <thead className="bg-card text-fg-faint text-[10px] uppercase tracking-wide">
                  <tr className="border-b border-neutral-800/40">
                    <th className="px-3 py-1.5 text-left font-medium">Signal</th>
                    <th className="px-3 py-1.5 text-left font-medium">Type</th>
                    <th className="px-3 py-1.5 text-right font-medium" title="Mean monthly rank IC">IC</th>
                    <th className="px-3 py-1.5 text-right font-medium" title="t-statistic of the IC">t</th>
                    <th className="px-3 py-1.5 text-right font-medium" title="Fraction of months with IC>0">Hit</th>
                    <th className="px-3 py-1.5 text-right font-medium" title="Top-minus-bottom quintile next-month return (winsorized)">Q-spread</th>
                    <th className="px-3 py-1.5 text-right font-medium" title="Decile-return monotonicity">Mono</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-neutral-800/20">
                  {data.signals.map((s) => (
                    <Fragment key={s.signal}>
                      <tr onClick={() => setOpen(open === s.signal ? null : s.signal)}
                        className="hover:bg-overlay/[0.02] cursor-pointer">
                        <td className="px-3 py-1.5 text-fg-soft whitespace-nowrap">
                          <span className="text-fg-faint text-[9px] mr-1">{open === s.signal ? '▾' : '▸'}</span>{s.label}
                          <CadenceTag cadence={s.cadence} />
                          {s.significant && <span className="ml-1.5 text-[8px] uppercase tracking-wider text-accent-400">sig</span>}
                        </td>
                        <td className="px-3 py-1.5">
                          <span className={`text-[9px] uppercase tracking-wider px-1 py-0.5 rounded ${GROUP_STYLE[s.group] ?? GROUP_STYLE.price}`}>{s.group}</span>
                        </td>
                        <td className="px-3 py-1.5 text-right font-mono" style={icStyle(s.mean_ic)}>{num(s.mean_ic, 3)}</td>
                        <td className={`px-3 py-1.5 text-right font-mono ${Math.abs(s.t_stat) >= 2 ? 'text-fg-strong font-semibold' : 'text-fg-muted'}`}>{s.t_stat.toFixed(1)}</td>
                        <td className="px-3 py-1.5 text-right font-mono text-fg-muted">{pct(s.hit_rate)}</td>
                        <td className="px-3 py-1.5 text-right font-mono text-fg-muted">{s.quintile_spread == null ? '—' : pct(s.quintile_spread)}</td>
                        <td className="px-3 py-1.5 text-right font-mono text-fg-muted">{num(s.monotonicity, 2)}</td>
                      </tr>
                      {open === s.signal && (
                        <tr>
                          <td colSpan={7} className="px-3 py-2 bg-inset/40">
                            <DecileBars deciles={s.deciles} />
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Per-sector IC heatmap — the "does momentum work in this sector?" view */}
        {data && sectors.length > 0 && (
          <div className="bg-card border border-neutral-800/40 rounded-xl p-4 space-y-2">
            <div className="text-sm font-semibold text-fg-strong">IC by sector</div>
            <p className="text-[10px] text-fg-faint">Mean IC of each signal within each sector · green = predictive, red = inverse · blank = too few names.</p>
            <div className="overflow-auto rounded-lg border border-neutral-800/40">
              <table className="text-[10px]">
                <thead className="bg-card sticky top-0">
                  <tr>
                    <th className="px-2 py-1.5 text-left font-medium text-fg-faint sticky left-0 bg-card z-10">Signal</th>
                    {sectors.map((sec) => (
                      <th key={sec} className="px-1.5 py-1.5 font-medium text-fg-faint whitespace-nowrap" title={sectorLabel(sec)}>
                        <div className="w-10 truncate">{sectorLabel(sec)}</div>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.signals.map((s) => (
                    <tr key={s.signal} className="border-t border-neutral-800/20">
                      <td className="px-2 py-1 text-fg-soft whitespace-nowrap sticky left-0 bg-card z-10">{s.label}<CadenceTag cadence={s.cadence} /></td>
                      {sectors.map((sec) => {
                        const ic = s.sector_ic?.[sec];
                        return (
                          <td key={sec} className="px-1 py-1 text-center font-mono text-fg-strong" style={icStyle(ic)} title={`${s.label} · ${sectorLabel(sec)}: IC ${ic == null ? 'n/a' : ic.toFixed(3)}`}>
                            {ic == null ? '' : (ic * 100).toFixed(0)}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="text-[9px] text-fg-faint">Values are IC × 100 (e.g. 5 = 0.05).</p>
          </div>
        )}

        {/* IC by regime — does the signal behave differently in bull/bear × calm/turbulent? */}
        {data && data.regime_months && (
          <div className="bg-card border border-neutral-800/40 rounded-xl p-4 space-y-2">
            <div className="text-sm font-semibold text-fg-strong">IC by regime</div>
            <p className="text-[10px] text-fg-faint">
              Mean IC in each market regime at decision time (causal). Watch the month count — a thin regime (few months) is noisy, so don’t over-trust it.
            </p>
            <div className="overflow-auto rounded-lg border border-neutral-800/40">
              <table className="w-full text-xs">
                <thead className="bg-card text-fg-faint text-[10px] uppercase tracking-wide">
                  <tr className="border-b border-neutral-800/40">
                    <th className="px-3 py-1.5 text-left font-medium">Signal</th>
                    {REGIME_ORDER.map((k) => (
                      <th key={k} className="px-3 py-1.5 text-right font-medium whitespace-nowrap">
                        <span className="inline-flex items-center gap-1 justify-end">
                          <span className="inline-block h-2 w-2 rounded-sm" style={{ background: REG[k].dot }} />
                          {REG[k].label}
                        </span>
                        <div className="text-fg-faint font-normal normal-case">{data.regime_months?.[k] ?? 0} mo</div>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-neutral-800/20">
                  {data.signals.map((s) => (
                    <tr key={s.signal} className="hover:bg-overlay/[0.02]">
                      <td className="px-3 py-1.5 text-fg-soft whitespace-nowrap">{s.label}<CadenceTag cadence={s.cadence} /></td>
                      {REGIME_ORDER.map((k) => {
                        const ic = s.regime_ic?.[k];
                        return (
                          <td key={k} className="px-3 py-1.5 text-right font-mono text-fg-strong" style={icStyle(ic)}
                            title={ic == null ? 'too few months' : `IC ${ic.toFixed(3)}`}>
                            {ic == null ? '—' : ic.toFixed(3)}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="text-[9px] text-fg-faint">
              A signal that flips sign (e.g. momentum positive in calm, negative in bear-turbulent) is a candidate for regime-conditioning — validate train/test before trusting it.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

/** Mean next-month return per signal-decile — a clean upward gradient = a good signal. */
function DecileBars({ deciles }: { deciles: (number | null)[] }) {
  const vals = deciles.filter((d): d is number => d != null);
  if (!vals.length) return <div className="text-[10px] text-fg-faint">No decile data.</div>;
  const max = Math.max(...vals.map(Math.abs)) || 1;
  return (
    <div>
      <div className="text-[10px] text-fg-muted mb-1">Next-month return by signal decile (low → high)</div>
      <div className="flex items-end gap-1 h-16">
        {deciles.map((d, i) => {
          const h = d == null ? 0 : (Math.abs(d) / max) * 100;
          const up = (d ?? 0) >= 0;
          return (
            <div key={i} className="flex-1 flex flex-col items-center justify-end h-full" title={d == null ? '—' : pct(d)}>
              <div className={`w-full rounded-sm ${up ? 'bg-pos-500/60' : 'bg-neg-500/60'}`} style={{ height: `${h}%` }} />
            </div>
          );
        })}
      </div>
      <div className="flex justify-between text-[9px] text-fg-faint mt-0.5"><span>D1</span><span>D10</span></div>
    </div>
  );
}
