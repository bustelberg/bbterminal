'use client';

import { useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';

/** One pickable instrument — identity only. Mirrors `AssetSearchRow` on the server. */
export type AssetPick = {
  isin: string;
  analysis_id?: number | null;
  name?: string | null;
  yahoo_symbol?: string | null;
  exchange?: string | null;
  currency?: string | null;
  sector?: string | null;
  bars?: number | null;
};

/**
 * Type-ahead over the asset pipeline, for picking ONE company.
 *
 * ⚠⚠ IT SEARCHES SERVER-SIDE, AND THAT IS NOT A PREFERENCE. `/api/asset-pipeline/grid` — what the
 * Asset Pipeline page loads — is **27.56 MB** for its 16,613 rows. Pulling that down to filter it
 * in the browser for a ten-row dropdown would be the single heaviest thing on this page, paid on
 * every visit, to show a name and an ISIN. `/api/asset-pipeline/search` answers in ~50 ms.
 *
 * ⚠ IT OFFERS ONLY WHAT CAN BE DRAWN. The server restricts to `status='ok'` rows with an
 * `analysis_id` and bars > 0 (~8,200 of 16,613). Half the grid is bonds, unresolved ISINs and
 * zero-bar rows; offering one of those would let someone pick a company and get an empty panel,
 * which reads as a broken page rather than as an instrument we cannot price.
 */
export default function CompanyPicker({ label, value, onPick }: {
  label: string;
  value: AssetPick | null;
  onPick: (a: AssetPick | null) => void;
}) {
  const [q, setQ] = useState('');
  const [rows, setRows] = useState<AssetPick[]>([]);
  const [truncated, setTruncated] = useState(false);
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const box = useRef<HTMLDivElement>(null);

  // ⚠ DEBOUNCED, AND THE IN-FLIGHT REQUEST IS ABORTED. Typing "nvidia" is six keystrokes; without
  // both of these it is six requests whose replies can land out of order, and the list settles on
  // whichever answered last rather than on what is in the box.
  useEffect(() => {
    const term = q.trim();
    if (term.length < 2) { setRows([]); setTruncated(false); return; }
    const ctrl = new AbortController();
    const t = setTimeout(() => {
      setBusy(true);
      void (async () => {
        try {
          const r = await apiFetch(
            `${API_URL}/api/asset-pipeline/search?q=${encodeURIComponent(term)}&limit=25`,
            { signal: ctrl.signal });
          const b = await r.json().catch(() => null);
          if (!r.ok) throw new Error((b?.detail as string) ?? `HTTP ${r.status}`);
          setRows((b?.rows ?? []) as AssetPick[]);
          setTruncated(Boolean(b?.truncated));
        } catch (e) {
          if (ctrl.signal.aborted) return;      // our own cancel, not a failure
          console.warn('[research] search failed:', e);
          setRows([]);
        } finally {
          if (!ctrl.signal.aborted) setBusy(false);
        }
      })();
    }, 200);
    return () => { clearTimeout(t); ctrl.abort(); };
  }, [q]);

  // Close on an outside click — a dropdown that only closes on selection traps the page.
  useEffect(() => {
    const onDoc = (e: MouseEvent) => {
      if (box.current && !box.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, []);

  return (
    <div ref={box} className="relative">
      <label className="block text-[10px] uppercase tracking-wide text-fg-faint mb-1">{label}</label>
      <div className="flex gap-2">
        <input
          value={q}
          onChange={(e) => { setQ(e.target.value); setOpen(true); }}
          onFocus={() => setOpen(true)}
          placeholder="Name, ISIN or ticker…"
          className="flex-1 min-w-0 bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg-strong placeholder-fg-faint focus:outline-none focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 transition-colors"
        />
        {value && (
          <button type="button" onClick={() => { onPick(null); setQ(''); }}
            title="Clear this side"
            className="shrink-0 px-3 rounded-lg border border-neutral-800/40 text-fg-faint hover:text-fg text-sm transition-colors">
            ✕
          </button>
        )}
      </div>

      {open && q.trim().length >= 2 && (
        <div className="absolute z-20 mt-1 w-full max-h-72 overflow-auto bg-popover border border-neutral-800/40 rounded-lg shadow-lg">
          {busy && !rows.length && <p className="px-3 py-2 text-xs text-fg-subtle">Searching…</p>}
          {!busy && !rows.length && (
            <p className="px-3 py-2 text-xs text-fg-subtle">
              Nothing priceable matches that. Only resolved instruments with price history can be
              charted.
            </p>
          )}
          {rows.map((r) => (
            <button key={`${r.isin}-${r.analysis_id ?? ''}`} type="button"
              onClick={() => { onPick(r); setOpen(false); setQ(r.name ?? r.isin); }}
              className="w-full text-left px-3 py-2 hover:bg-overlay/[0.04] transition-colors border-b border-neutral-800/20 last:border-0">
              <div className="text-sm text-fg-strong truncate">{r.name ?? r.isin}</div>
              <div className="text-[11px] text-fg-faint font-mono truncate">
                {r.isin}
                {r.yahoo_symbol && ` · ${r.yahoo_symbol}`}
                {r.exchange && ` · ${r.exchange}`}
                {r.currency && ` · ${r.currency}`}
                {/* ⚠ THE BAR COUNT IS SHOWN because it is what the list is ORDERED by, and an
                    order the reader cannot see reads as arbitrary. It is also the honest way to
                    tell a company's main listing from a thin foreign one carrying the same name. */}
                {typeof r.bars === 'number' && ` · ${r.bars.toLocaleString('en-US')} bars`}
              </div>
            </button>
          ))}
          {truncated && (
            // ⚠ SAID, NOT SILENTLY DROPPED. A capped list that does not admit it invites the
            // reader to conclude their company is not in the pipeline.
            <p className="px-3 py-2 text-[11px] text-fg-faint border-t border-neutral-800/20">
              More matches than shown — keep typing to narrow it.
            </p>
          )}
        </div>
      )}
    </div>
  );
}
