'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { runSSE } from '../../lib/stream';
import { sectorLabel } from '../../lib/assetLabels';
import AssetNav from './AssetNav';
import LwLineChart from './LwLineChart';
import PerfTables from './alphalab/PerfTables';
import SectorBreakdown, { type SectorIndex } from './alphalab/SectorBreakdown';
import RegimeLegend from './alphalab/RegimeLegend';
import { type RegimeData, buildRegimeBands } from './alphalab/regimeBands';
import { RANGES, type RangeId } from './alphalab/window';

// Persisted benchmark preferences. Commodities structurally underperform, so a
// strategy could "beat" a commodity-laden benchmark just by avoiding them —
// exclude them from the bar by default. Editable + saved to localStorage.
const EXCLUDE_KEY = 'alphalab.excludeSectors';
const DEFAULT_EXCLUDED = ['commodity'];
// Regime-overlay enabler — bull/bear × calm/turbulent bands drawn behind
// every chart. Saved so the preference sticks.
const OVERLAY_KEY = 'alphalab.regimeOverlay';

// Train/test split (shared config in ./alphalab/window). Each window loads only
// its own data (+ a warm-up buffer the backend adds and slices off), so you tune
// the detector on the training set and validate out-of-sample on the test set.
const RANGE_KEY = 'alphalab.range';

/** AlphaLab — chart the equal-weight price index of a saved universe (=100 at the
 * start), on the same TradingView chart engine as the execution-instruments page. */
export default function AlphaLab() {
  const [savedUniverses, setSavedUniverses] = useState<{ id: number; name: string; ticker_count: number }[]>([]);
  const [universeId, setUniverseId] = useState('');
  const [regime, setRegime] = useState<RegimeData | null>(null);
  const [sectors, setSectors] = useState<SectorIndex[] | null>(null);
  const [sectorsLoading, setSectorsLoading] = useState(false);
  const [excluded, setExcluded] = useState<Set<string>>(() => new Set(DEFAULT_EXCLUDED));
  const [overlay, setOverlay] = useState(true);
  const [range, setRange] = useState<RangeId>('train');
  const [prefsLoaded, setPrefsLoaded] = useState(false);
  const [loading, setLoading] = useState(false);
  const [stage, setStage] = useState<string | null>(null);
  const [elapsed, setElapsed] = useState(0);
  const [error, setError] = useState<string | null>(null);

  const excludeKey = useMemo(() => [...excluded].sort().join(','), [excluded]);

  // Hydrate the saved exclusions once (in an effect, not the initializer, to
  // avoid an SSR/client hydration mismatch), then persist on every change.
  useEffect(() => {
    try {
      const raw = localStorage.getItem(EXCLUDE_KEY);
      if (raw) setExcluded(new Set(JSON.parse(raw) as string[]));
      const ov = localStorage.getItem(OVERLAY_KEY);
      if (ov != null) setOverlay(ov === 'true');
      const rg = localStorage.getItem(RANGE_KEY);
      if (rg === 'train' || rg === 'test' || rg === 'full') setRange(rg);
    } catch { /* ignore */ }
    setPrefsLoaded(true);
  }, []);
  useEffect(() => {
    if (!prefsLoaded) return;
    try { localStorage.setItem(EXCLUDE_KEY, JSON.stringify([...excluded])); } catch { /* ignore */ }
  }, [excluded, prefsLoaded]);
  useEffect(() => {
    if (!prefsLoaded) return;
    try { localStorage.setItem(OVERLAY_KEY, String(overlay)); } catch { /* ignore */ }
  }, [overlay, prefsLoaded]);
  useEffect(() => {
    if (!prefsLoaded) return;
    try { localStorage.setItem(RANGE_KEY, range); } catch { /* ignore */ }
  }, [range, prefsLoaded]);

  // Tick an elapsed-seconds counter while a compute is in flight. The regime
  // endpoint is one blocking COPY load (no per-step progress to stream), so an
  // indeterminate bar + elapsed time is the honest "it's working" signal.
  useEffect(() => {
    if (!loading) { setElapsed(0); return; }
    const t0 = performance.now();
    const id = setInterval(() => setElapsed((performance.now() - t0) / 1000), 250);
    return () => clearInterval(id);
  }, [loading]);

  useEffect(() => {
    (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/asset-pipeline/universes`);
        const b = await r.json().catch(() => null);
        if (r.ok) setSavedUniverses(b?.universes ?? []);
      } catch { /* ignore */ }
    })();
  }, []);

  // Overall benchmark index — refetched when the universe OR the excluded
  // sectors change. Streamed (SSE) so the UI can show the live compute stage;
  // no `start`: begin at the earliest date any member has data (the equal-weight
  // index averages only the assets available on each day).
  const loadRegime = useCallback(async (id: string, exclude: string, start: string | undefined, end: string | undefined, signal: AbortSignal) => {
    setLoading(true); setError(null); setRegime(null); setStage(null);
    try {
      const p = new URLSearchParams({ universe_id: id });
      if (exclude) p.set('exclude_sectors', exclude);
      if (start) p.set('start', start);
      if (end) p.set('end', end);
      await runSSE(`${API_URL}/api/asset-pipeline/alphalab/regime/stream?${p}`, { method: 'GET' }, (data) => {
        if (signal.aborted) return;
        const d = data as { stage?: string; error?: string; result?: RegimeData & { note?: string } };
        if (d.error) { setError(d.error); return; }
        if (d.stage === 'done') {
          const b = d.result;
          if (!b?.dates?.length) setError(b?.note ?? 'No price history for this universe.');
          else setRegime(b);
        } else if (d.stage) {
          setStage(d.stage);
        }
      }, signal);
    } catch (e) {
      if (!signal.aborted) setError(e instanceof Error ? e.message : String(e));
    } finally {
      if (!signal.aborted) { setLoading(false); setStage(null); }
    }
  }, []);

  // Per-sector breakdown — depends only on the universe (shows ALL sectors,
  // including excluded ones, so you can decide what to drop). Streamed so cards
  // render progressively (largest sector first) rather than all at once. Reuses
  // the cached price panel primed by the benchmark stream, so no second COPY.
  const loadSectors = useCallback(async (id: string, start: string | undefined, end: string | undefined, signal: AbortSignal) => {
    setSectors([]); setSectorsLoading(true);
    try {
      const p = new URLSearchParams({ universe_id: id });
      if (start) p.set('start', start);
      if (end) p.set('end', end);
      await runSSE(`${API_URL}/api/asset-pipeline/alphalab/sectors/stream?${p}`, { method: 'GET' }, (data) => {
        if (signal.aborted) return;
        const d = data as { topic?: string; result?: SectorIndex };
        if (d.topic === 'sector' && d.result) {
          setSectors((prev) => [...(prev ?? []), d.result!]);
        }
      }, signal);
    } catch { /* ignore (incl. abort) */ } finally {
      if (!signal.aborted) setSectorsLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!prefsLoaded) return;
    if (!universeId) { setRegime(null); setError(null); return; }
    const ac = new AbortController();
    const r = RANGES[range];
    void loadRegime(universeId, excludeKey, r.start, r.end, ac.signal);
    return () => ac.abort();
  }, [universeId, excludeKey, range, prefsLoaded, loadRegime]);

  useEffect(() => {
    if (!universeId) { setSectors(null); setSectorsLoading(false); return; }
    const ac = new AbortController();
    const r = RANGES[range];
    void loadSectors(universeId, r.start, r.end, ac.signal);
    return () => ac.abort();
  }, [universeId, range, loadSectors]);

  const toggleExcluded = (sector: string) =>
    setExcluded((prev) => {
      const n = new Set(prev);
      if (n.has(sector)) n.delete(sector); else n.add(sector);
      return n;
    });

  // Equal-weight index, rebased to 100 at the window start.
  const eqLine = useMemo(() => {
    const idx = regime?.index;
    if (!idx?.length) return [];
    const base = idx[0] || 1;
    return regime!.dates.map((d, i) => ({ date: d, value: (idx[i] / base) * 100 }));
  }, [regime]);

  const excludedList = useMemo(
    () => [...excluded].map((s) => sectorLabel(s)).sort().join(', '),
    [excluded],
  );

  // Regime bands for the main chart overlay (only when enabled + available).
  const regimeBands = useMemo(
    () => (overlay && regime?.bull?.length ? buildRegimeBands(regime.dates, regime.bull, regime.turb) : undefined),
    [overlay, regime],
  );

  return (
    <div className="min-h-screen bg-page text-fg">
      <AssetNav />
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">AlphaLab</h1>
        <p className="text-sm text-fg-subtle mt-1">Equal-weight price index of a saved universe, indexed to 100 at the start.</p>
      </div>

      <div className="px-8 py-6 space-y-4">
        <div className="bg-card border border-neutral-800/40 rounded-xl p-4 space-y-3">
          {/* Train/test window selector — tune on training, validate on test. */}
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
            <span className="text-[10px] text-fg-faint">tune the detector on the training set · validate out-of-sample on the test set</span>
          </div>

          <div className="flex items-center gap-3 flex-wrap">
            <span className="text-xs font-semibold text-fg-strong uppercase tracking-wide">Universe</span>
            <select value={universeId} onChange={(e) => setUniverseId(e.target.value)}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-sm max-w-[280px] focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30">
              <option value="">Select a universe…</option>
              {savedUniverses.map((u) => <option key={u.id} value={u.id}>{u.name} ({u.ticker_count.toLocaleString()})</option>)}
            </select>
            {loading && <span className="text-[11px] text-fg-faint">Computing… {stage ? `${stage} · ` : ''}{elapsed.toFixed(1)}s</span>}
            <label className="flex items-center gap-1.5 text-[11px] text-fg-muted cursor-pointer ml-auto"
              title="Overlay bull/bear × calm/turbulent regime bands behind every chart">
              <input type="checkbox" checked={overlay} onChange={(e) => setOverlay(e.target.checked)}
                className="accent-accent-500 h-3.5 w-3.5" />
              Regime overlay
            </label>
          </div>

          {/* Indeterminate progress — the compute is a single blocking request,
              so there's no % to report; the bar signals liveness. */}
          {loading && <div className="loading-bar h-0.5 w-full rounded-full" aria-hidden />}

          {error &&<div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>}
          {regime && eqLine.length > 0 && (
            <>
              {overlay && regimeBands && <RegimeLegend current={regime.current} />}
              <LwLineChart data={eqLine} scale="log" unit="=100" bands={regimeBands} />
              <div className="text-[10px] text-fg-faint">
                {RANGES[range].label} set ({RANGES[range].span}) · equal-weight index of {regime.universe?.name ? `“${regime.universe.name}”` : 'the universe'} ({regime.universe?.size ?? 0} instruments)
                {excluded.size > 0 && ` · excludes ${excludedList}`}
                {' · =100 at start · scroll to zoom, drag to pan, double-click to reset'}
                {overlay && regimeBands && ' · background = bull/bear × calm/turbulent regime'}.
              </div>
            </>
          )}
          {!regime && !error && !loading && <p className="text-[11px] text-fg-subtle">Pick a universe to chart its equal-weight index.</p>}

          {/* Benchmark sector exclusions — saved to localStorage. */}
          {sectors && sectors.length > 0 && (
            <div className="space-y-1.5 pt-1">
              <div className="text-[11px] text-fg-muted">
                Benchmark sectors <span className="text-fg-faint">— click to exclude a sector from the index you’re trying to beat (saved)</span>
              </div>
              <div className="flex flex-wrap gap-1.5">
                {sectors.map((s) => {
                  const off = excluded.has(s.sector);
                  return (
                    <button key={s.sector} type="button" onClick={() => toggleExcluded(s.sector)}
                      title={off ? 'Excluded — click to include' : 'Included — click to exclude'}
                      className={`text-[11px] px-2 py-0.5 rounded-full border transition-colors ${
                        off
                          ? 'border-neutral-700 text-fg-faint line-through opacity-60 hover:opacity-90'
                          : 'border-accent-500/40 bg-accent-500/10 text-accent-400 hover:bg-accent-500/15'
                      }`}>
                      {sectorLabel(s.sector)} <span className="opacity-60 font-mono">{s.size}</span>
                    </button>
                  );
                })}
              </div>
            </div>
          )}
        </div>

        {regime && regime.index?.length > 1 && (
          <div className="bg-card border border-neutral-800/40 rounded-xl p-4 space-y-3">
            <div className="text-sm font-semibold text-fg-strong">Risk &amp; return by period</div>
            <PerfTables dates={regime.dates} level={regime.index} />
          </div>
        )}

        {((sectors && sectors.length > 0) || sectorsLoading) && (
          <div className="bg-card border border-neutral-800/40 rounded-xl p-4">
            <SectorBreakdown sectors={sectors ?? []} loading={sectorsLoading} overlay={overlay} />
          </div>
        )}
      </div>
    </div>
  );
}
