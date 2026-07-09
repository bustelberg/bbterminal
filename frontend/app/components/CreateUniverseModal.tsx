'use client';

import type { ReactNode } from 'react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { sectorLabel } from '../../lib/assetLabels';
import { dialog } from '../../lib/dialog';

export type SavedUniverse = { id: number; name: string; ticker_count: number };

const INPUT_CLS = 'bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 w-28 text-right';

// Module-scoped so it isn't a fresh component type each render (which would
// remount — and drop focus from — the inputs on every keystroke).
function Field({ label, hint, children }: { label: string; hint?: string; children: ReactNode }) {
  return (
    <div className="space-y-0.5">
      <label className="flex items-center justify-between gap-3 text-xs text-fg-muted">
        <span>{label}</span>{children}
      </label>
      {hint && <p className="text-[10px] text-fg-faint leading-tight">{hint}</p>}
    </div>
  );
}

/** Build + save a named liquid universe of unique yfinance tickers. Tune the
 * identity/liquidity params, watch the live count, then Create. */
export default function CreateUniverseModal({ sectorOptions, universes, onDeleted, onClose }: {
  sectorOptions: string[];
  universes: SavedUniverse[];
  onDeleted: () => void;
  onClose: (created: boolean) => void;
}) {
  const [name, setName] = useState('');
  const [minAdvM, setMinAdvM] = useState(1);        // €M — fallback liquidity gate for cap-less names
  const [minMktCapB, setMinMktCapB] = useState(1);  // €B — matches LEONTEQ_MIN_MARKET_CAP_EUR (mid-cap+ floor)
  const [maxZeroPct, setMaxZeroPct] = useState(5);  // %
  const [reqLeonteq, setReqLeonteq] = useState(true);
  const [reqOpenfigi, setReqOpenfigi] = useState(true);
  const [reqVolume, setReqVolume] = useState(true);
  const [sectors, setSectors] = useState<Set<string>>(() => new Set(sectorOptions)); // all ticked

  const [count, setCount] = useState<number | null>(null);
  const [counting, setCounting] = useState(false);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const allSectors = sectors.size === sectorOptions.length;
  const noSectors = sectors.size === 0;

  const params = useMemo(() => {
    const q = new URLSearchParams({
      min_adv_eur: String(Math.max(0, minAdvM) * 1e6),
      min_market_cap_eur: String(Math.max(0, minMktCapB) * 1e9),
      max_zero_vol: String(Math.min(1, Math.max(0, maxZeroPct / 100))),
      require_leonteq: String(reqLeonteq),
      require_openfigi_match: String(reqOpenfigi),
      require_volume: String(reqVolume),
    });
    // Only send `sectors` for a proper subset — all ticked = every sector.
    if (!allSectors && !noSectors) q.set('sectors', [...sectors].join(','));
    return q;
  }, [minAdvM, minMktCapB, maxZeroPct, reqLeonteq, reqOpenfigi, reqVolume, sectors, allSectors, noSectors]);

  // Live count (debounced) — count_only so we don't ship the whole list.
  useEffect(() => {
    if (noSectors) { setCount(0); setCounting(false); return; }
    let alive = true;
    setCounting(true);
    const t = setTimeout(async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/asset-pipeline/universe?${params}&count_only=true`);
        const b = await r.json().catch(() => null);
        if (alive && r.ok) setCount(b?.count ?? null);
      } catch { /* ignore */ } finally { if (alive) setCounting(false); }
    }, 350);
    return () => { alive = false; clearTimeout(t); };
  }, [params, noSectors]);

  const toggleSector = (s: string) =>
    setSectors((prev) => { const n = new Set(prev); if (n.has(s)) n.delete(s); else n.add(s); return n; });

  const create = async () => {
    if (!name.trim() || !count) return;
    setCreating(true); setError(null);
    try {
      const body = {
        name: name.trim(),
        min_adv_eur: Math.max(0, minAdvM) * 1e6,
        min_market_cap_eur: Math.max(0, minMktCapB) * 1e9,
        max_zero_vol: Math.min(1, Math.max(0, maxZeroPct / 100)),
        require_leonteq: reqLeonteq, require_openfigi_match: reqOpenfigi,
        require_volume: reqVolume,
        sectors: (allSectors || noSectors) ? null : [...sectors],
      };
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/universe/create`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
      });
      const b = await r.json().catch(() => null);
      if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
      onClose(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setCreating(false); }
  };

  const [deletingId, setDeletingId] = useState<number | null>(null);
  // Only close on a backdrop click that also STARTED on the backdrop — otherwise
  // drag-selecting text in an input (mouse-up landing on the backdrop) closes it.
  const downOnBackdrop = useRef(false);

  const remove = async (u: SavedUniverse) => {
    const ok = await dialog.confirm(
      `Delete the universe "${u.name}" (${u.ticker_count.toLocaleString()} tickers)? This cannot be undone.`,
      { title: 'Delete universe', confirmLabel: 'Delete', destructive: true },
    );
    if (!ok) return;
    setDeletingId(u.id); setError(null);
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/universes/${u.id}`, { method: 'DELETE' });
      if (!r.ok) {
        const b = await r.json().catch(() => null);
        setError(b?.detail ?? `HTTP ${r.status}`);
        return;
      }
      onDeleted();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setDeletingId(null); }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-scrim/60 p-4"
      onMouseDown={(e) => { downOnBackdrop.current = e.target === e.currentTarget; }}
      onClick={(e) => { if (e.target === e.currentTarget && downOnBackdrop.current) onClose(false); }}>
      <div className="bg-elevated border border-neutral-800/40 rounded-xl shadow-xl w-full max-w-md p-5 space-y-4"
        onClick={(e) => e.stopPropagation()}>
        <h3 className="text-sm font-semibold text-fg-strong">Create liquid universe</h3>

        <input value={name} onChange={(e) => setName(e.target.value)} placeholder="Universe name…"
          className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg w-full focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30" />

        <div className="space-y-2.5">
          <Field label="Min market cap (€B)" hint="Primary gate for names WITH a market cap (listing-independent). 0 = no floor; 1 = large-cap.">
            <input type="number" min={0} step={1} value={minMktCapB} onChange={(e) => setMinMktCapB(+e.target.value)} className={INPUT_CLS} />
          </Field>
          <Field label="Min ADV (€M)" hint="Fallback gate for cap-less names (ETFs/crypto). 1 = ~€1M traded/day.">
            <input type="number" min={0} step={0.5} value={minAdvM} onChange={(e) => setMinAdvM(+e.target.value)} className={INPUT_CLS} />
          </Field>
          <Field label="Max zero-vol (%)" hint="5 = typical (tolerates holidays). 1 = strictest (daily-traded only).">
            <input type="number" min={0} max={100} step={1} value={maxZeroPct} onChange={(e) => setMaxZeroPct(+e.target.value)} className={INPUT_CLS} />
          </Field>
          <Field label="Require Leonteq (4 cols)">
            <input type="checkbox" checked={reqLeonteq} onChange={(e) => setReqLeonteq(e.target.checked)} className="accent-accent-500 h-4 w-4" />
          </Field>
          <Field label="Require OpenFIGI match">
            <input type="checkbox" checked={reqOpenfigi} onChange={(e) => setReqOpenfigi(e.target.checked)} className="accent-accent-500 h-4 w-4" />
          </Field>
          <Field label="Require volume data">
            <input type="checkbox" checked={reqVolume} onChange={(e) => setReqVolume(e.target.checked)} className="accent-accent-500 h-4 w-4" />
          </Field>
        </div>

        {/* Sectors — all ticked by default; untick to exclude. */}
        <div className="space-y-1.5">
          <div className="flex items-center justify-between text-xs text-fg-muted">
            <span>Sectors ({sectors.size}/{sectorOptions.length})</span>
            <span className="flex gap-2">
              <button type="button" onClick={() => setSectors(new Set(sectorOptions))} className="text-accent-400 hover:underline">All</button>
              <button type="button" onClick={() => setSectors(new Set())} className="text-accent-400 hover:underline">None</button>
            </span>
          </div>
          <div className="max-h-36 overflow-auto rounded-lg border border-neutral-800/40 p-2 grid grid-cols-2 gap-x-3 gap-y-1">
            {sectorOptions.map((s) => (
              <label key={s} className="flex items-center gap-2 text-xs text-fg-soft cursor-pointer">
                <input type="checkbox" checked={sectors.has(s)} onChange={() => toggleSector(s)} className="accent-accent-500 h-3.5 w-3.5 shrink-0" />
                <span className="truncate">{sectorLabel(s)}</span>
              </label>
            ))}
          </div>
        </div>

        {/* Existing universes — inspect count + delete. */}
        {universes.length > 0 && (
          <div className="space-y-1.5">
            <div className="text-xs text-fg-muted">Existing universes ({universes.length})</div>
            <div className="max-h-32 overflow-auto rounded-lg border border-neutral-800/40 divide-y divide-neutral-800/20">
              {universes.map((u) => (
                <div key={u.id} className="group flex items-center justify-between gap-2 px-2.5 py-1.5 text-xs">
                  <span className="truncate text-fg-soft">{u.name}</span>
                  <span className="flex items-center gap-2 shrink-0">
                    <span className="font-mono text-fg-faint">{u.ticker_count.toLocaleString()}</span>
                    <button type="button" onClick={() => void remove(u)} disabled={deletingId !== null}
                      title="Delete universe"
                      className="text-neg-400 hover:text-neg-300 disabled:opacity-40 disabled:cursor-not-allowed transition-colors opacity-0 group-hover:opacity-100">
                      {deletingId === u.id ? '…' : '✕'}
                    </button>
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="text-center py-2 rounded-lg bg-accent-500/10 border border-accent-500/20">
          <span className="text-lg font-semibold text-accent-300 font-mono">
            {counting ? '…' : (count?.toLocaleString() ?? '—')}
          </span>
          <span className="text-xs text-fg-muted ml-2">unique yfinance tickers</span>
        </div>

        {error && <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>}

        <div className="flex justify-end gap-2 pt-1">
          <button type="button" onClick={() => onClose(false)}
            className="text-xs px-4 py-2 rounded-lg border border-neutral-700 text-fg-muted hover:text-fg-strong transition-colors">Cancel</button>
          <button type="button" onClick={() => void create()} disabled={creating || !name.trim() || !count}
            className="text-xs px-4 py-2 rounded-lg bg-accent-600 hover:bg-accent-500 text-white disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
            {creating ? 'Creating…' : `Create${count ? ` (${count.toLocaleString()})` : ''}`}
          </button>
        </div>
      </div>
    </div>
  );
}
