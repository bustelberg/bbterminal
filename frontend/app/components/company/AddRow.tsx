'use client';

import { useEffect, useRef, useState } from 'react';
import Spinner from '../Spinner';
import { API_URL } from '../../../lib/apiUrl';
import { apiFetch } from '../../../lib/apiFetch';
import { inputAddCls } from './styles';
import type { DupeMatch } from './types';

// ─── Add new company row ──────────────────────────────────────────────────────

export default function AddRow({
  exchangeOptions,
  onAdd,
  onCancel,
}: {
  exchangeOptions: string[];
  onAdd: (c: { company_name: string; gurufocus_ticker: string; gurufocus_exchange: string }) => Promise<void>;
  onCancel: () => void;
}) {
  const [name, setName] = useState('');
  const [ticker, setTicker] = useState('');
  const [exchange, setExchange] = useState('');
  const [saving, setSaving] = useState(false);
  // Inline duplicate-detection state. Empty = no probe yet; the API
  // call fires when name AND (ticker OR exchange) are populated. The
  // canonical_ticker echo shows the form the row would actually be
  // stored as — `00700` rather than the `700` the user typed.
  const [dupeMatches, setDupeMatches] = useState<DupeMatch[]>([]);
  const [canonicalTicker, setCanonicalTicker] = useState<string>('');
  const [dupesLoading, setDupesLoading] = useState(false);
  const nameRef = useRef<HTMLInputElement>(null);

  useEffect(() => { nameRef.current?.focus(); }, []);

  // Debounced probe — 300ms so each keystroke doesn't hit the API.
  useEffect(() => {
    const n = name.trim();
    const t = ticker.trim();
    const e = exchange.trim();
    if (!n && !t) {
      setDupeMatches([]);
      setCanonicalTicker('');
      return;
    }
    const handle = window.setTimeout(async () => {
      setDupesLoading(true);
      try {
        const params = new URLSearchParams({ name: n, ticker: t, exchange: e });
        const res = await apiFetch(`${API_URL}/api/companies/check-duplicates?${params}`);
        if (!res.ok) return;
        const body = await res.json();
        setDupeMatches(body.matches ?? []);
        setCanonicalTicker(body.canonical_ticker ?? '');
      } catch {
        // Network or backend error — silently leave matches empty;
        // the POST will surface a 409 if there's a real conflict.
      } finally {
        setDupesLoading(false);
      }
    }, 300);
    return () => window.clearTimeout(handle);
  }, [name, ticker, exchange]);

  async function handleAdd() {
    if (!name.trim() || !ticker.trim() || !exchange.trim()) return;
    setSaving(true);
    await onAdd({ company_name: name.trim(), gurufocus_ticker: ticker.trim(), gurufocus_exchange: exchange.trim() });
    setSaving(false);
  }

  const tickerLooksDifferent = canonicalTicker && canonicalTicker !== ticker.trim().toUpperCase();
  const hasMatches = dupeMatches.length > 0;

  return (
    <>
    <tr className="border-b border-pos-800/20 bg-pos-500/5">
      <td className="px-4 py-2 text-sm text-fg-faint">
        {dupesLoading ? (
          <span className="inline-flex items-center" title="Checking for duplicate companies…">
            <Spinner size={10} className="h-2.5 w-2.5 text-pos-500/80" />
          </span>
        ) : (
          'new'
        )}
      </td>
      <td className="px-3 py-2"><input ref={nameRef} value={name} onChange={(e) => setName(e.target.value)} placeholder="Company name" className={inputAddCls} /></td>
      <td className="px-3 py-2">
        <input value={ticker} onChange={(e) => setTicker(e.target.value)} placeholder="TICKER" className={inputAddCls} />
        {tickerLooksDifferent && (
          <div className="text-[10px] text-warn-400 font-mono mt-0.5" title="HKSE tickers are stored zero-padded to 5 digits">
            → stored as {canonicalTicker}
          </div>
        )}
      </td>
      <td className="px-3 py-2">
        <input list="add-exchange" value={exchange} onChange={(e) => setExchange(e.target.value)} placeholder="EXCHANGE" className={inputAddCls} />
        <datalist id="add-exchange">{exchangeOptions.map((o) => <option key={o} value={o} />)}</datalist>
      </td>
      <td className="px-3 py-2 text-sm text-fg-faint">—</td>
      <td className="px-3 py-2 text-sm text-fg-faint">—</td>
      <td className="px-3 py-2 text-sm text-fg-faint">—</td>
      <td className="px-3 py-2 text-right text-sm text-fg-faint">—</td>
      <td className="px-3 py-2 text-sm text-fg-faint">—</td>
      <td className="px-3 py-2">
        <div className="flex gap-1.5">
          <button onClick={handleAdd} disabled={saving || !name.trim() || !ticker.trim() || !exchange.trim()} className="px-3 py-1.5 rounded-lg text-xs font-medium bg-pos-600 hover:bg-pos-500 text-fg-strong disabled:opacity-50 transition-colors inline-flex items-center gap-1.5">
            {saving && <Spinner size={12} className="h-3 w-3 text-fg-strong" />}
            {saving ? 'Adding…' : hasMatches ? 'Add anyway' : 'Add'}
          </button>
          <button onClick={onCancel} className="px-3 py-1.5 rounded-lg text-xs font-medium text-fg-muted hover:text-fg-strong hover:bg-overlay/5 transition-colors">
            Cancel
          </button>
        </div>
      </td>
    </tr>
    {hasMatches && (
      <tr className="border-b border-warn-800/20 bg-warn-500/5">
        <td colSpan={10} className="px-4 py-3">
          <div className="text-xs text-warn-300 font-medium mb-2 flex items-center gap-2">
            <span>
              ⚠ {dupeMatches.length} possible duplicate{dupeMatches.length === 1 ? '' : 's'} already in the database
            </span>
            {dupesLoading && (
              <span className="inline-flex items-center gap-1 text-fg-subtle">
                <Spinner size={10} className="h-2.5 w-2.5 text-fg-subtle" />
                <span>re-checking…</span>
              </span>
            )}
          </div>
          <ul className="space-y-1">
            {dupeMatches.map((m) => (
              <li key={m.company_id} className="text-xs text-fg-soft font-mono flex items-center gap-3">
                <span className="text-fg-subtle">cid={m.company_id}</span>
                <span className="text-fg-muted">{m.gurufocus_exchange ?? '?'}:{m.gurufocus_ticker}</span>
                <span className="text-fg">{m.company_name}</span>
              </li>
            ))}
          </ul>
          <div className="text-[11px] text-fg-subtle mt-2">
            Click <span className="text-warn-300">Add anyway</span> to create a new row regardless, or <span className="text-fg-soft">Cancel</span> and use the existing match.
          </div>
        </td>
      </tr>
    )}
    </>
  );
}
