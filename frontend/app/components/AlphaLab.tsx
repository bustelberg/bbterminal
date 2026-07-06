'use client';

import { useCallback, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import AssetNav from './AssetNav';

type Sig = {
  signal: string;
  mean_ic: number;
  t_stat: number;
  p_value: number;
  hit_rate: number;
  quintile_spread: number | null;
  months: number;
  significant: boolean;
};
type Sector = { sector: string; count: number };
type Universe = { size: number; matched: number; sectors: Sector[] };
type Board = {
  universe: Universe;
  months: number;
  from?: string;
  to?: string;
  signals: Sig[];
  note?: string;
};

const pct = (x: number | null | undefined, d = 1) =>
  x === null || x === undefined ? '—' : `${(x * 100).toFixed(d)}%`;

const ADV_OPTIONS = [
  { v: 0, label: 'Any liquidity' },
  { v: 100_000, label: '≥ €100k ADV' },
  { v: 1_000_000, label: '≥ €1M ADV' },
  { v: 5_000_000, label: '≥ €5M ADV' },
  { v: 10_000_000, label: '≥ €10M ADV' },
];

const SIGNAL_LABELS: Record<string, string> = {
  mom_12_1: '12-1 momentum',
  mom_6_1: '6-1 momentum',
  mom_3m: '3-month momentum',
  reversal_1m: '1-month reversal',
  vol_adj_12_1: 'vol-adjusted 12-1',
  dist_from_high_12m: 'proximity to 12m high',
};

export default function AlphaLab() {
  const [minAdv, setMinAdv] = useState(1_000_000);
  const [assetClass, setAssetClass] = useState('equity');
  const [requireSector, setRequireSector] = useState(true);
  const [maxAssets, setMaxAssets] = useState(600);

  const [universe, setUniverse] = useState<Universe | null>(null); // cheap preview
  const [board, setBoard] = useState<Board | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const qs = useCallback((preview: boolean, refresh: boolean) => {
    const p = new URLSearchParams({
      min_adv_eur: String(minAdv),
      require_sector: String(requireSector),
      asset_class: assetClass,
      max_assets: String(maxAssets),
    });
    if (preview) p.set('preview', 'true');
    if (refresh) p.set('refresh', 'true');
    return p.toString();
  }, [minAdv, requireSector, assetClass, maxAssets]);

  // Cheap debounced preview — universe size + sector breakdown as filters change
  // (no price load, so it's instant).
  useEffect(() => {
    const t = setTimeout(async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/asset-pipeline/alphalab?${qs(true, false)}`);
        const b = await r.json().catch(() => null);
        if (r.ok && b?.universe) setUniverse(b.universe);
      } catch { /* ignore */ }
    }, 400);
    return () => clearTimeout(t);
  }, [qs]);

  const run = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/alphalab?${qs(false, false)}`);
      const b = await r.json().catch(() => null);
      if (!r.ok) setError(b?.detail ?? `HTTP ${r.status}`);
      else { setBoard(b as Board); if (b?.universe) setUniverse(b.universe); }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setLoading(false); }
  }, [qs]);

  return (
    <div className="min-h-screen bg-page text-fg">
      <AssetNav />
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">AlphaLab</h1>
        <p className="text-sm text-fg-subtle mt-1">
          Define a sensible universe of analysis instruments (liquidity floor · has a sector · asset class), then measure which
          signals actually predict returns on it — each signal&apos;s cross-sectional <span className="font-medium text-fg">Information Coefficient</span> (monthly
          rank correlation with next-month return), with t-stat, hit rate and top−bottom quintile spread.
        </p>
      </div>

      <div className="px-8 py-6 space-y-4">
        {/* Universe definition */}
        <div className="bg-card border border-neutral-800/40 rounded-xl p-4 space-y-3">
          <div className="flex items-center gap-3 flex-wrap">
            <span className="text-xs font-semibold text-fg-strong uppercase tracking-wide">Universe</span>
            <select value={minAdv} onChange={(e) => setMinAdv(Number(e.target.value))}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-sm focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30">
              {ADV_OPTIONS.map((o) => <option key={o.v} value={o.v}>{o.label}</option>)}
            </select>
            <select value={assetClass} onChange={(e) => setAssetClass(e.target.value)}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-sm focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30">
              <option value="equity">Equities</option>
              <option value="">All classes</option>
            </select>
            <label className="text-sm text-fg-muted flex items-center gap-1.5">
              <input type="checkbox" checked={requireSector} onChange={(e) => setRequireSector(e.target.checked)}
                className="accent-accent-600" />
              has a sector
            </label>
            <label className="text-sm text-fg-muted flex items-center gap-2">
              max
              <input type="number" min={20} max={2500} step={50} value={maxAssets}
                onChange={(e) => setMaxAssets(Math.max(20, Math.min(2500, Number(e.target.value) || 600)))}
                className="w-24 bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-sm font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30" />
            </label>
            <button type="button" onClick={() => void run()} disabled={loading}
              className="text-sm px-4 py-2 rounded-lg bg-accent-600 hover:bg-accent-500 text-white disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
              {loading ? 'Computing…' : 'Run scoreboard'}
            </button>
          </div>

          {universe && (
            <div className="flex items-center gap-2 flex-wrap text-[11px]">
              <span className="font-mono text-fg-soft">
                {universe.size.toLocaleString()} instruments
                {universe.matched > universe.size && <span className="text-fg-faint"> (top {universe.size} of {universe.matched.toLocaleString()} matching)</span>}
              </span>
              {universe.sectors.slice(0, 10).map((s) => (
                <span key={s.sector} className="px-1.5 py-0.5 rounded bg-inset text-fg-muted">{s.sector} <span className="text-fg-faint">{s.count}</span></span>
              ))}
            </div>
          )}
        </div>

        {error && <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3 text-sm text-neg-300">{error}</div>}
        {board?.note && <div className="bg-warn-500/10 border border-warn-500/20 rounded-lg px-4 py-3 text-sm text-warn-300">{board.note}</div>}

        {board && board.signals.length > 0 && (
          <>
            <div className="text-[11px] text-fg-faint font-mono">
              {board.universe.size} instruments · {board.months} months · {board.from} → {board.to}
            </div>
            <div className="bg-card border border-neutral-800/40 rounded-xl overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-fg-muted border-b border-neutral-800/40">
                    <th className="px-4 py-3 font-medium">Signal</th>
                    <th className="px-4 py-3 font-medium text-right">Mean IC</th>
                    <th className="px-4 py-3 font-medium text-right">t-stat</th>
                    <th className="px-4 py-3 font-medium text-right">p-value</th>
                    <th className="px-4 py-3 font-medium text-right">Hit rate</th>
                    <th className="px-4 py-3 font-medium text-right">Q5−Q1 spread</th>
                    <th className="px-4 py-3 font-medium text-right">Months</th>
                  </tr>
                </thead>
                <tbody>
                  {board.signals.map((s) => (
                    <tr key={s.signal} className="border-b border-neutral-800/20 hover:bg-overlay/[0.02] transition-colors">
                      <td className="px-4 py-2.5">
                        <span className="text-fg-strong">{SIGNAL_LABELS[s.signal] ?? s.signal}</span>
                        {s.significant && <span className="ml-2 text-[10px] px-1.5 py-0.5 rounded bg-accent-500/15 text-accent-300">|t|≥2</span>}
                        <div className="text-[10px] text-fg-faint font-mono">{s.signal}</div>
                      </td>
                      <td className={`px-4 py-2.5 text-right font-mono ${s.mean_ic > 0 ? 'text-pos-400' : s.mean_ic < 0 ? 'text-neg-400' : 'text-fg-muted'}`}>{s.mean_ic.toFixed(4)}</td>
                      <td className={`px-4 py-2.5 text-right font-mono ${s.significant ? 'text-fg-strong font-medium' : 'text-fg-muted'}`}>{s.t_stat > 0 ? '+' : ''}{s.t_stat.toFixed(2)}</td>
                      <td className="px-4 py-2.5 text-right font-mono text-fg-muted">{s.p_value.toFixed(3)}</td>
                      <td className="px-4 py-2.5 text-right font-mono text-fg">{pct(s.hit_rate, 0)}</td>
                      <td className={`px-4 py-2.5 text-right font-mono ${(s.quintile_spread ?? 0) > 0 ? 'text-pos-400' : (s.quintile_spread ?? 0) < 0 ? 'text-neg-400' : 'text-fg-muted'}`}>{pct(s.quintile_spread, 2)}</td>
                      <td className="px-4 py-2.5 text-right font-mono text-fg-faint">{s.months}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}

        <p className="text-[11px] text-fg-faint max-w-3xl">
          IC is the mean monthly Spearman rank correlation between the signal and the next month&apos;s return across the universe; a |t-stat| ≥ 2
          (~p &lt; 0.05) flags a signal whose edge is unlikely to be noise. Q5−Q1 is the average monthly return gap between the top and bottom signal
          quintiles. Defining a liquid, sectored equity universe strips out FX/crypto/illiquid noise so the IC reflects a tradable edge. This is a
          first cut — the full etoro alpha-lab adds FDR-corrected bootstrap confidence, tradability net of spreads, robustness and decile-monotonicity gates.
        </p>
      </div>
    </div>
  );
}
