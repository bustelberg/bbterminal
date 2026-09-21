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

type ExternalAsset = {
  symbol: string;
  name: string;
  isin?: string | null;
  exchange?: string | null;
  currency?: string | null;
  sector?: string | null;
};

/**
 * Type-ahead over the asset pipeline, for picking ONE company.
 *
 *  It searches server-side, and that is not a preference. `/api/asset-pipeline/grid` — what the
 * Asset Pipeline page loads — is **27.56 MB** for its 16,613 rows. Pulling that down to filter it
 * in the browser for a ten-row dropdown would be the single heaviest thing on this page, paid on
 * every visit, to show a name and an ISIN. `/api/asset-pipeline/search` answers in ~50 ms.
 *
 *  It offers only what can be drawn. The server restricts to `status='ok'` rows with an
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
  const [externalRows, setExternalRows] = useState<ExternalAsset[]>([]);
  const [truncated, setTruncated] = useState(false);
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [selectedExternal, setSelectedExternal] = useState<ExternalAsset | null>(null);
  const [isin, setIsin] = useState('');
  const [adding, setAdding] = useState(false);
  const [addError, setAddError] = useState('');
  const box = useRef<HTMLDivElement>(null);

  //  Debounced, and the in-flight request is aborted. Typing "nvidia" is six keystrokes; without
  // both of these it is six requests whose replies can land out of order, and the list settles on
  // whichever answered last rather than on what is in the box.
  useEffect(() => {
    const term = q.trim();
    if (term.length < 2) { setRows([]); setExternalRows([]); setTruncated(false); return; }
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
          const localRows = (b?.rows ?? []) as AssetPick[];
          setRows(localRows);
          setTruncated(Boolean(b?.truncated));
          if (localRows.length) {
            setExternalRows([]);
            return;
          }
          const external = await apiFetch(
            `${API_URL}/api/asset-pipeline/external-search?q=${encodeURIComponent(term)}`,
            { signal: ctrl.signal });
          const externalBody = await external.json().catch(() => null);
          if (!external.ok) throw new Error((externalBody?.detail as string) ?? `HTTP ${external.status}`);
          setExternalRows((externalBody?.rows ?? []) as ExternalAsset[]);
        } catch (e) {
          if (ctrl.signal.aborted) return;      // our own cancel, not a failure
          console.warn('[research] search failed:', e);
          setRows([]); setExternalRows([]);
        } finally {
          if (!ctrl.signal.aborted) setBusy(false);
        }
      })();
    }, 200);
    return () => { clearTimeout(t); ctrl.abort(); };
  }, [q]);

  const addExternal = async () => {
    if (!selectedExternal || !isin.trim()) return;
    setAdding(true); setAddError('');
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/external-store`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ symbol: selectedExternal.symbol, isin: isin.trim() }),
      });
      const body = await r.json().catch(() => null);
      if (!r.ok) throw new Error((body?.detail as string) ?? `HTTP ${r.status}`);
      onPick(body as AssetPick);
      setOpen(false); setSelectedExternal(null); setIsin('');
    } catch (e) {
      setAddError(e instanceof Error ? e.message : 'Could not add this company.');
    } finally {
      setAdding(false);
    }
  };

  // Close on an outside click — a dropdown that only closes on selection traps the page.
  useEffect(() => {
    const onDoc = (e: MouseEvent) => {
      if (box.current && !box.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, []);

  if (value) {
    return (
      <div>
        <div className="mb-1.5 text-sm font-medium text-fg-soft">{label}</div>
        <div className="flex min-h-10 items-center gap-3 rounded-lg border border-neutral-800/40 bg-card px-3 py-2">
          <div className="min-w-0 flex-1">
            <div className="truncate text-sm text-fg-strong">{value.name ?? value.isin}</div>
            <div className="truncate font-mono text-[11px] text-fg-faint">{value.isin}</div>
          </div>
          <button
            type="button"
            onClick={() => { onPick(null); setQ(''); setSelectedExternal(null); setIsin(''); setAddError(''); }}
            className="shrink-0 text-xs text-fg-subtle transition-colors hover:text-fg"
          >
            Change
          </button>
        </div>
      </div>
    );
  }

  return (
    <div ref={box} className="relative">
      {/*  BIGGER, AND NO LONGER UPPERCASED — the two go together. At 10px small-caps this was
          furniture; at a readable size the same styling shouts "COMPANY A — THE SUBJECT" at a
          reader who is being asked a question, which is the reasoning `OwnerEarningsModal` already
          records about its own eyebrow: size and ink set a line apart, and small caps on top of
          that is decoration that stops being harmless once the line carries a sentence. */}
      <label className="block text-sm font-medium text-fg-soft mb-1.5">{label}</label>
      <div className="flex gap-2">
        <input
          value={q}
          onChange={(e) => { setQ(e.target.value); setOpen(true); setSelectedExternal(null); setIsin(''); setAddError(''); }}
          onFocus={() => setOpen(true)}
          placeholder="Name, ISIN or ticker…"
          className="flex-1 min-w-0 bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg-strong placeholder-fg-faint focus:outline-none focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 transition-colors"
        />
      </div>

      {open && q.trim().length >= 2 && (
        <div className="absolute z-20 mt-1 w-full max-h-72 overflow-auto bg-popover border border-neutral-800/40 rounded-lg shadow-lg">
          {busy && !rows.length && <p className="px-3 py-2 text-xs text-fg-subtle">Searching…</p>}
          {!busy && !rows.length && !externalRows.length && (
            <p className="px-3 py-2 text-xs text-fg-subtle">
              No matching company was found.
            </p>
          )}
          {rows.map((r) => (
            <button key={`${r.isin}-${r.analysis_id ?? ''}`} type="button"
              onMouseDown={(e) => { e.preventDefault(); e.stopPropagation(); onPick(r); setOpen(false); setQ(r.name ?? r.isin); }}
              onClick={() => { onPick(r); setOpen(false); setQ(r.name ?? r.isin); }}
              className="w-full cursor-pointer text-left px-3 py-2 hover:bg-overlay/[0.04] transition-colors border-b border-neutral-800/20 last:border-0">
              <div className="text-sm text-fg-strong truncate">{r.name ?? r.isin}</div>
              <div className="text-[11px] text-fg-faint font-mono truncate">{r.isin}</div>
            </button>
          ))}
          {selectedExternal && (
            <div className="border-t border-neutral-800/20 p-3 space-y-2 bg-overlay/[0.02]">
              <p className="text-xs text-fg-subtle">
                {selectedExternal.isin
                  ? <>We found ISIN <span className="font-mono text-fg-strong">{selectedExternal.isin}</span> for {selectedExternal.name}.</>
                  : <>Enter the ISIN for <span className="text-fg-strong">{selectedExternal.name}</span>.</>}
                {' '}We will verify it matches {selectedExternal.symbol} before saving.
              </p>
              <div className="flex gap-2">
                <input value={isin} onChange={(e) => setIsin(e.target.value.toUpperCase())}
                  placeholder="ISIN, e.g. US0378331005" maxLength={12}
                  className="min-w-0 flex-1 bg-page border border-neutral-700 rounded-lg px-2.5 py-1.5 text-xs text-fg-strong placeholder-fg-faint focus:outline-none focus:border-accent-500" />
                <button type="button" disabled={adding || !isin.trim()} onClick={() => void addExternal()}
                  className="shrink-0 rounded-lg bg-accent-600 px-3 py-1.5 text-xs font-medium text-white disabled:opacity-50">
                  {adding ? 'Verifying…' : 'Add'}
                </button>
              </div>
              {addError && <p className="text-xs text-red-400">{addError}</p>}
            </div>
          )}
          {!rows.length && externalRows.map((r) => (
            <button key={r.symbol} type="button"
              onMouseDown={(e) => { e.preventDefault(); e.stopPropagation(); setSelectedExternal(r); setIsin(r.isin ?? ''); setAddError(''); }}
              onClick={() => { setSelectedExternal(r); setIsin(r.isin ?? ''); setAddError(''); }}
              className="w-full cursor-pointer text-left px-3 py-2 hover:bg-overlay/[0.04] transition-colors border-b border-neutral-800/20 last:border-0">
              <div className="text-sm text-fg-strong truncate">{r.name}</div>
              <div className="text-[11px] text-fg-faint font-mono truncate">
                {r.exchange ? `${r.exchange}: ` : ''}{r.symbol} · Add to research
              </div>
            </button>
          ))}
          {truncated && (
            //  Said, not silently dropped. A capped list that does not admit it invites the
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
