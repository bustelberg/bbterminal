'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import AssetNav from './AssetNav';
import LwLineChart from './LwLineChart';
import { type RegimeData } from './RegimeChart';

/** AlphaLab — chart the equal-weight price index of a saved universe (=100 at the
 * start), on the same TradingView chart engine as the execution-instruments page. */
export default function AlphaLab() {
  const [savedUniverses, setSavedUniverses] = useState<{ id: number; name: string; ticker_count: number }[]>([]);
  const [universeId, setUniverseId] = useState('');
  const [regime, setRegime] = useState<RegimeData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/asset-pipeline/universes`);
        const b = await r.json().catch(() => null);
        if (r.ok) setSavedUniverses(b?.universes ?? []);
      } catch { /* ignore */ }
    })();
  }, []);

  const load = useCallback(async (id: string) => {
    if (!id) { setRegime(null); setError(null); return; }
    setLoading(true); setError(null); setRegime(null);
    try {
      const p = new URLSearchParams({ universe_id: id, start: '2015-01-01' });
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/alphalab/regime?${p}`);
      const b = await r.json().catch(() => null);
      if (!r.ok) setError(b?.detail ?? `HTTP ${r.status}`);
      else if (!b?.dates?.length) setError(b?.note ?? 'No price history for this universe.');
      else setRegime(b as RegimeData);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setLoading(false); }
  }, []);

  // Equal-weight index, rebased to 100 at the window start.
  const eqLine = useMemo(() => {
    const idx = regime?.index;
    if (!idx?.length) return [];
    const base = idx[0] || 1;
    return regime!.dates.map((d, i) => ({ date: d, value: (idx[i] / base) * 100 }));
  }, [regime]);

  return (
    <div className="min-h-screen bg-page text-fg">
      <AssetNav />
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">AlphaLab</h1>
        <p className="text-sm text-fg-subtle mt-1">Equal-weight price index of a saved universe, indexed to 100 at the start.</p>
      </div>

      <div className="px-8 py-6 space-y-4">
        <div className="bg-card border border-neutral-800/40 rounded-xl p-4 space-y-3">
          <div className="flex items-center gap-3 flex-wrap">
            <span className="text-xs font-semibold text-fg-strong uppercase tracking-wide">Universe</span>
            <select value={universeId} onChange={(e) => { setUniverseId(e.target.value); void load(e.target.value); }}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-sm max-w-[280px] focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30">
              <option value="">Select a universe…</option>
              {savedUniverses.map((u) => <option key={u.id} value={u.id}>{u.name} ({u.ticker_count.toLocaleString()})</option>)}
            </select>
            {loading && <span className="text-[11px] text-fg-faint">Computing…</span>}
          </div>

          {error && <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>}
          {regime && eqLine.length > 0 && (
            <>
              <LwLineChart data={eqLine} scale="log" unit="=100" />
              <div className="text-[10px] text-fg-faint">
                Equal-weight index of {regime.universe?.name ? `“${regime.universe.name}”` : 'the universe'} ({regime.universe?.size ?? 0} instruments)
                · =100 at start · scroll to zoom, drag to pan, double-click to reset.
              </div>
            </>
          )}
          {!regime && !error && !loading && <p className="text-[11px] text-fg-subtle">Pick a universe to chart its equal-weight index.</p>}
        </div>
      </div>
    </div>
  );
}
