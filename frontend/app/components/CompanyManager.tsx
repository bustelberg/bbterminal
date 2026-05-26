'use client';

import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { dialog } from '../../lib/dialog';
import { guruFocusUrl } from '../../lib/gurufocusUrl';
import { useClickOutside, useEscapeKey } from '../../lib/hooks/useClickOutside';
import { useIsAdmin } from '../../lib/hooks/useEffectiveRole';
import { trackedFetch } from '../../lib/loading';
import type { Column } from '../../lib/tableExport';
import TableDownloadButton from './TableDownloadButton';
import LoadingDots from './LoadingDots';
import Spinner from './Spinner';

import { API_URL } from '../../lib/apiUrl';

type Company = {
  company_id: number;
  company_name: string | null;
  gurufocus_ticker: string;
  gurufocus_exchange: string;
  country: string | null;
  universes: string[];
  /** ISO timestamp set by the price phase when GuruFocus returns "delisted"
   * or "stock not found" for this (ticker, exchange). Companies with a
   * non-null value are excluded from the backtest gap warning and the
   * pipeline skips them entirely on subsequent runs. */
  delisted_at?: string | null;
  /** ISO timestamp set when GuruFocus returns "Stock not found" on the
   * primary exchange AND every fallback. Typically means the row's
   * exchange is wrong (e.g. NYSE:ASND when it should be NASDAQ:ASND).
   * UI renders a red "GF lookup" badge + a 'Find correct exchange'
   * button that probes the GuruFocus diagnostic endpoint. Cleared
   * automatically the next time a price fetch succeeds. */
  gurufocus_lookup_failed_at?: string | null;
};

type SortField = 'company_name' | 'gurufocus_ticker' | 'gurufocus_exchange' | 'country';
type SortDir = 'asc' | 'desc';

// Deterministic hue per universe label so the same universe always gets the
// same chip colour across renders. Cheap string hash → 0-359 hue, with fixed
// saturation + lightness tuned for legibility on the dark theme.
function hashHue(label: string): number {
  let h = 0;
  for (let i = 0; i < label.length; i++) h = (h * 31 + label.charCodeAt(i)) | 0;
  return Math.abs(h) % 360;
}

function universeChipStyle(label: string): React.CSSProperties {
  const hue = hashHue(label);
  return {
    backgroundColor: `hsl(${hue} 70% 22% / 0.55)`,
    borderColor: `hsl(${hue} 70% 45% / 0.55)`,
    color: `hsl(${hue} 80% 78%)`,
  };
}

const inputCls = 'w-full bg-[#0f1117] border border-gray-700 rounded-lg px-2.5 py-1.5 text-sm text-white focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors';
const inputAddCls = 'w-full bg-[#0f1117] border border-emerald-800/50 rounded-lg px-2.5 py-1.5 text-sm text-white focus:outline-none focus:border-emerald-500 focus:ring-1 focus:ring-emerald-500/30 transition-colors';

// ─── Multi-select checklist filter ───────────────────────────────────────────
// Replaces the single-select dropdowns so the universe filter can pick out
// e.g. ACWI ∩ LEONTEQ by checking both. `combineMode` is purely cosmetic —
// it shows "(AND)" / "(OR)" in the panel header so the user knows whether
// two checked entries narrow (AND) or widen (OR) the result. The actual
// AND/OR application lives in the caller's filter useMemo.
function MultiSelectFilter({
  label,
  options,
  selected,
  onChange,
  combineMode,
}: {
  label: string;
  options: string[];
  selected: string[];
  onChange: (next: string[]) => void;
  combineMode?: 'AND' | 'OR';
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const ref = useRef<HTMLDivElement>(null);
  useClickOutside(ref, () => setOpen(false), open);
  useEscapeKey(() => setOpen(false), open);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return q ? options.filter((o) => o.toLowerCase().includes(q)) : options;
  }, [options, query]);

  const buttonLabel =
    selected.length === 0
      ? `All ${label.toLowerCase()}`
      : selected.length <= 2
      ? selected.join(', ')
      : `${selected.length} ${label.toLowerCase()}`;

  const toggle = (opt: string) => {
    onChange(
      selected.includes(opt)
        ? selected.filter((s) => s !== opt)
        : [...selected, opt],
    );
  };

  return (
    <div ref={ref} className="relative">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className={`bg-[#151821] border rounded-lg px-3 py-2 text-sm text-white transition-colors inline-flex items-center gap-2 ${
          selected.length > 0
            ? 'border-indigo-500/60 text-indigo-200'
            : 'border-gray-800/60 hover:border-gray-700'
        }`}
      >
        <span className="truncate max-w-[180px]">{buttonLabel}</span>
        <svg
          className={`w-3.5 h-3.5 text-gray-500 transition-transform ${open ? 'rotate-180' : ''}`}
          viewBox="0 0 20 20"
          fill="currentColor"
        >
          <path
            fillRule="evenodd"
            d="M5.23 7.21a.75.75 0 011.06.02L10 11.06l3.71-3.83a.75.75 0 111.08 1.04l-4.25 4.39a.75.75 0 01-1.08 0L5.21 8.27a.75.75 0 01.02-1.06z"
            clipRule="evenodd"
          />
        </svg>
      </button>
      {open && (
        <div className="absolute left-0 mt-1 w-64 bg-[#151821] border border-gray-700 rounded-lg shadow-xl z-50 max-h-80 overflow-hidden flex flex-col">
          <div className="px-3 py-2 border-b border-gray-800/60 flex items-center justify-between gap-2">
            <span className="text-xs text-gray-400">
              {label}
              {combineMode && selected.length >= 2 && (
                <span className="ml-1.5 text-[10px] uppercase tracking-wide text-gray-600">
                  ({combineMode})
                </span>
              )}
            </span>
            {selected.length > 0 && (
              <button
                type="button"
                onClick={() => onChange([])}
                className="text-[11px] text-gray-500 hover:text-white"
              >
                Clear
              </button>
            )}
          </div>
          {options.length > 8 && (
            <div className="px-2 pt-2">
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Filter…"
                className="w-full bg-[#0f1117] border border-gray-700 rounded px-2 py-1 text-xs text-white placeholder-gray-600 focus:outline-none focus:border-indigo-500"
              />
            </div>
          )}
          <div className="flex-1 overflow-auto p-1">
            {filtered.length === 0 ? (
              <div className="px-3 py-2 text-xs text-gray-600">No matches</div>
            ) : (
              filtered.map((opt) => {
                const checked = selected.includes(opt);
                return (
                  <label
                    key={opt}
                    className={`flex items-center gap-2 px-2 py-1.5 rounded hover:bg-white/[0.04] cursor-pointer text-sm ${
                      checked ? 'text-white' : 'text-gray-300'
                    }`}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggle(opt)}
                      className="accent-indigo-500 w-3.5 h-3.5"
                    />
                    <span className="truncate">{opt}</span>
                  </label>
                );
              })
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Inline edit row ──────────────────────────────────────────────────────────

function EditRow({
  company,
  exchangeOptions,
  onSave,
  onCancel,
}: {
  company: Company;
  exchangeOptions: string[];
  onSave: (updated: Partial<Company>) => Promise<void>;
  onCancel: () => void;
}) {
  const [name, setName] = useState(company.company_name ?? '');
  const [ticker, setTicker] = useState(company.gurufocus_ticker);
  const [exchange, setExchange] = useState(company.gurufocus_exchange);
  const [saving, setSaving] = useState(false);

  async function handleSave() {
    setSaving(true);
    await onSave({ company_name: name, gurufocus_ticker: ticker, gurufocus_exchange: exchange });
    setSaving(false);
  }

  return (
    <tr className="border-b border-gray-800/30 bg-indigo-500/5">
      <td className="px-4 py-2 text-sm text-gray-500">{company.company_id}</td>
      <td className="px-3 py-2"><input value={name} onChange={(e) => setName(e.target.value)} className={inputCls} /></td>
      <td className="px-3 py-2"><input value={ticker} onChange={(e) => setTicker(e.target.value)} className={inputCls} /></td>
      <td className="px-3 py-2">
        <input list="edit-exchange" value={exchange} onChange={(e) => setExchange(e.target.value)} className={inputCls} />
        <datalist id="edit-exchange">{exchangeOptions.map((o) => <option key={o} value={o} />)}</datalist>
      </td>
      <td className="px-3 py-2 text-gray-400">{company.country ?? '—'}</td>
      <td className="px-3 py-2 text-gray-600 text-xs">—</td>
      <td className="px-3 py-2">
        <div className="flex gap-1.5">
          <button onClick={handleSave} disabled={saving} className="px-3 py-1.5 rounded-lg text-xs font-medium bg-indigo-600 hover:bg-indigo-500 text-white disabled:opacity-50 transition-colors inline-flex items-center gap-1.5">
            {saving && <Spinner size={12} className="h-3 w-3 text-white" />}
            {saving ? 'Saving…' : 'Save'}
          </button>
          <button onClick={onCancel} className="px-3 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition-colors">
            Cancel
          </button>
        </div>
      </td>
    </tr>
  );
}

// ─── Add new company row ──────────────────────────────────────────────────────

type DupeMatch = {
  company_id: number;
  company_name: string | null;
  gurufocus_ticker: string;
  gurufocus_exchange: string | null;
};

function AddRow({
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
        const res = await fetch(`${API_URL}/api/companies/check-duplicates?${params}`);
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
    <tr className="border-b border-emerald-800/20 bg-emerald-500/5">
      <td className="px-4 py-2 text-sm text-gray-600">
        {dupesLoading ? (
          <span className="inline-flex items-center" title="Checking for duplicate companies…">
            <Spinner size={10} className="h-2.5 w-2.5 text-emerald-500/80" />
          </span>
        ) : (
          'new'
        )}
      </td>
      <td className="px-3 py-2"><input ref={nameRef} value={name} onChange={(e) => setName(e.target.value)} placeholder="Company name" className={inputAddCls} /></td>
      <td className="px-3 py-2">
        <input value={ticker} onChange={(e) => setTicker(e.target.value)} placeholder="TICKER" className={inputAddCls} />
        {tickerLooksDifferent && (
          <div className="text-[10px] text-amber-400 font-mono mt-0.5" title="HKSE tickers are stored zero-padded to 5 digits">
            → stored as {canonicalTicker}
          </div>
        )}
      </td>
      <td className="px-3 py-2">
        <input list="add-exchange" value={exchange} onChange={(e) => setExchange(e.target.value)} placeholder="EXCHANGE" className={inputAddCls} />
        <datalist id="add-exchange">{exchangeOptions.map((o) => <option key={o} value={o} />)}</datalist>
      </td>
      <td className="px-3 py-2 text-sm text-gray-600">—</td>
      <td className="px-3 py-2 text-sm text-gray-600">—</td>
      <td className="px-3 py-2">
        <div className="flex gap-1.5">
          <button onClick={handleAdd} disabled={saving || !name.trim() || !ticker.trim() || !exchange.trim()} className="px-3 py-1.5 rounded-lg text-xs font-medium bg-emerald-600 hover:bg-emerald-500 text-white disabled:opacity-50 transition-colors inline-flex items-center gap-1.5">
            {saving && <Spinner size={12} className="h-3 w-3 text-white" />}
            {saving ? 'Adding…' : hasMatches ? 'Add anyway' : 'Add'}
          </button>
          <button onClick={onCancel} className="px-3 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition-colors">
            Cancel
          </button>
        </div>
      </td>
    </tr>
    {hasMatches && (
      <tr className="border-b border-amber-800/20 bg-amber-500/5">
        <td colSpan={7} className="px-4 py-3">
          <div className="text-xs text-amber-300 font-medium mb-2 flex items-center gap-2">
            <span>
              ⚠ {dupeMatches.length} possible duplicate{dupeMatches.length === 1 ? '' : 's'} already in the database
            </span>
            {dupesLoading && (
              <span className="inline-flex items-center gap-1 text-gray-500">
                <Spinner size={10} className="h-2.5 w-2.5 text-gray-500" />
                <span>re-checking…</span>
              </span>
            )}
          </div>
          <ul className="space-y-1">
            {dupeMatches.map((m) => (
              <li key={m.company_id} className="text-xs text-gray-300 font-mono flex items-center gap-3">
                <span className="text-gray-500">cid={m.company_id}</span>
                <span className="text-gray-400">{m.gurufocus_exchange ?? '?'}:{m.gurufocus_ticker}</span>
                <span className="text-gray-200">{m.company_name}</span>
              </li>
            ))}
          </ul>
          <div className="text-[11px] text-gray-500 mt-2">
            Click <span className="text-amber-300">Add anyway</span> to create a new row regardless, or <span className="text-gray-300">Cancel</span> and use the existing match.
          </div>
        </td>
      </tr>
    )}
    </>
  );
}

// ─── Main ─────────────────────────────────────────────────────────────────────

export default function CompanyManager() {
  const [companies, setCompanies] = useState<Company[]>([]);
  const [loading, setLoading] = useState(true);
  // Universe memberships are fetched as a second, slower roundtrip after the
  // companies list lands. While this is true, the Memberships column shows a
  // small spinner instead of "—" so an empty chip cell isn't mistaken for
  // "this company belongs to no universes".
  const [membershipsLoading, setMembershipsLoading] = useState(true);
  // company_id whose Delete request is currently in flight, or null.
  const [deletingId, setDeletingId] = useState<number | null>(null);
  const [search, setSearch] = useState('');
  // Multi-select filters. Exchange / Country combine as OR (a company has
  // exactly one of each, so AND would always return empty as soon as 2+
  // are checked). Universe combines as AND so the user can pick the
  // intersection of multiple memberships (e.g. ACWI ∩ LEONTEQ).
  const [filterExchange, setFilterExchange] = useState<string[]>([]);
  const [filterCountry, setFilterCountry] = useState<string[]>([]);
  const [filterUniverse, setFilterUniverse] = useState<string[]>([]);
  const [filterDupes, setFilterDupes] = useState(false);
  const [sortField, setSortField] = useState<SortField>('company_name');
  const [sortDir, setSortDir] = useState<SortDir>('asc');
  const [editingId, setEditingId] = useState<number | null>(null);
  const [adding, setAdding] = useState(false);
  const [pendingAdd, setPendingAdd] = useState<{
    company_name: string;
    gurufocus_ticker: string;
    gurufocus_exchange: string;
  } | null>(null);
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Mutation controls (Add / Edit / Delete) are admin-only. Read paths
  // — sort, search, filters, universe chips — stay open to everyone.
  const isAdmin = useIsAdmin();

  const load = useCallback(async () => {
    setLoading(true);
    setMembershipsLoading(true);
    try {
      const res = await trackedFetch('Loading companies', `${API_URL}/api/companies`);
      const data: Company[] = await res.json();
      // Companies render immediately with empty memberships. The slower
      // membership aggregate fires in parallel and merges in when ready.
      setCompanies(data.map((c) => ({ ...c, universes: c.universes ?? [] })));
    } catch {
      setError('Failed to load companies');
    }
    setLoading(false);

    try {
      const res = await trackedFetch(
        'Loading universe memberships',
        `${API_URL}/api/companies/memberships`,
      );
      const { memberships } = (await res.json()) as { memberships: Record<string, string[]> };
      setCompanies((prev) =>
        prev.map((c) => ({ ...c, universes: memberships[String(c.company_id)] ?? [] })),
      );
    } catch {
      // Non-fatal — chips just don't render.
    } finally {
      setMembershipsLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const exchangeOptions = useMemo(() => {
    const s = new Set(companies.map((c) => c.gurufocus_exchange).filter(Boolean));
    return [...s].sort();
  }, [companies]);

  const countryOptions = useMemo(() => {
    const s = new Set(companies.map((c) => c.country).filter((v): v is string => !!v?.trim()));
    return [...s].sort();
  }, [companies]);

  const universeOptions = useMemo(() => {
    const s = new Set<string>();
    for (const c of companies) for (const u of c.universes ?? []) s.add(u);
    return [...s].sort();
  }, [companies]);

  // Detect duplicate company names (case-insensitive)
  const duplicateNames = useMemo(() => {
    const counts = new Map<string, number>();
    for (const c of companies) {
      const name = (c.company_name ?? '').toLowerCase().trim();
      if (name) counts.set(name, (counts.get(name) ?? 0) + 1);
    }
    const dupes = new Set<string>();
    for (const [name, count] of counts) {
      if (count > 1) dupes.add(name);
    }
    return dupes;
  }, [companies]);

  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    let list = companies;
    if (q) {
      list = list.filter(
        (c) =>
          (c.company_name ?? '').toLowerCase().includes(q) ||
          c.gurufocus_ticker.toLowerCase().includes(q) ||
          c.gurufocus_exchange.toLowerCase().includes(q),
      );
    }
    if (filterExchange.length > 0) {
      list = list.filter((c) => filterExchange.includes(c.gurufocus_exchange));
    }
    if (filterCountry.length > 0) {
      list = list.filter((c) => c.country != null && filterCountry.includes(c.country));
    }
    if (filterUniverse.length > 0) {
      list = list.filter((c) => {
        const us = c.universes ?? [];
        return filterUniverse.every((u) => us.includes(u));
      });
    }
    if (filterDupes) {
      const nameCounts = new Map<string, number>();
      for (const c of companies) {
        const name = (c.company_name ?? '').trim().toLowerCase();
        if (name) nameCounts.set(name, (nameCounts.get(name) ?? 0) + 1);
      }
      list = list.filter((c) => {
        const name = (c.company_name ?? '').trim().toLowerCase();
        return name && (nameCounts.get(name) ?? 0) > 1;
      });
    }

    return [...list].sort((a, b) => {
      const av = (a[sortField] ?? '') as string;
      const bv = (b[sortField] ?? '') as string;
      const cmp = av.localeCompare(bv, undefined, { sensitivity: 'base' });
      return sortDir === 'asc' ? cmp : -cmp;
    });
  }, [companies, search, filterExchange, filterCountry, filterUniverse, filterDupes, sortField, sortDir]);

  // Count of companies that share a name with at least one other company.
  // Click the badge in the header to filter the table to just these rows.
  const duplicateCount = useMemo(() => {
    const nameCounts = new Map<string, number>();
    for (const c of companies) {
      const name = (c.company_name ?? '').trim().toLowerCase();
      if (name) nameCounts.set(name, (nameCounts.get(name) ?? 0) + 1);
    }
    let n = 0;
    for (const c of companies) {
      const name = (c.company_name ?? '').trim().toLowerCase();
      if (name && (nameCounts.get(name) ?? 0) > 1) n++;
    }
    return n;
  }, [companies]);

  function handleSort(field: SortField) {
    if (sortField === field) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortField(field);
      setSortDir('asc');
    }
  }

  async function handleSave(id: number, updated: Partial<Company>) {
    setError(null);
    try {
      const res = await apiFetch(`${API_URL}/api/companies/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updated),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail ?? `HTTP ${res.status}`);
      }
      const saved = await res.json();
      setCompanies((prev) => prev.map((c) => (c.company_id === id ? { ...c, ...saved } : c)));
      setEditingId(null);
    } catch (e) {
      setError(`Save failed: ${e instanceof Error ? e.message : e}`);
    }
  }

  async function handleAdd(data: { company_name: string; gurufocus_ticker: string; gurufocus_exchange: string }) {
    setError(null);
    setPendingAdd(data);
  }

  async function confirmAdd() {
    if (!pendingAdd) return;
    setConfirming(true);
    setError(null);
    try {
      // First try without `force`. The backend's canonical dupe check
      // returns 409 with the matching rows; on 409 we surface the
      // matches through a confirm dialog and retry with `force=true`.
      let res = await apiFetch(`${API_URL}/api/companies`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(pendingAdd),
      });
      if (res.status === 409) {
        const body = await res.json().catch(() => ({}));
        type Match = { company_id: number; company_name: string | null; gurufocus_ticker: string; gurufocus_exchange: string | null };
        const matches: Match[] = (body.detail?.matches ?? []) as Match[];
        const lines = matches.map((m) => `  cid=${m.company_id} ${m.gurufocus_exchange ?? '?'}:${m.gurufocus_ticker}  ${m.company_name ?? ''}`).join('\n');
        const proceed = await dialog.confirm(
          `${matches.length} possible duplicate${matches.length === 1 ? '' : 's'} found:\n\n${lines}\n\nAdd as a new company anyway?`,
          { destructive: true, confirmLabel: 'Add anyway' },
        );
        if (!proceed) {
          setPendingAdd(null);
          return;
        }
        res = await apiFetch(`${API_URL}/api/companies`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...pendingAdd, force: true }),
        });
      }
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail ?? `HTTP ${res.status}`);
      }
      setPendingAdd(null);
      setAdding(false);
      await load();
    } catch (e) {
      setError(`Add failed: ${e instanceof Error ? e.message : e}`);
    } finally {
      setConfirming(false);
    }
  }

  /** Probe GuruFocus across a list of candidate exchanges to find which
   * one actually resolves for this company's ticker. Surfaces the result
   * via `dialog` so the user sees a clear "FOUND on NASDAQ" / "NOT FOUND"
   * message + a one-click 'Update exchange to X' confirmation. The
   * update writes through the same PUT /api/companies/{id} the inline
   * edit uses, so the row refreshes naturally. */
  async function findCorrectExchange(c: Company) {
    try {
      const res = await apiFetch(`${API_URL}/api/admin/gurufocus-exchange-search`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tickers: [{ ticker: c.gurufocus_ticker, current_exchange: c.gurufocus_exchange }],
        }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        await dialog.alert(`Lookup failed: ${body.detail ?? `HTTP ${res.status}`}`);
        return;
      }
      const data = (await res.json()) as Array<{
        ticker: string;
        current_exchange: string | null;
        found_exchange: string | null;
        status: 'found' | 'not_found';
        candidates_tried: { exchange: string; status_code: number | null; ok: boolean }[];
        error: string | null;
      }>;
      const r = data[0];
      if (!r) {
        await dialog.alert('Lookup returned no result.');
        return;
      }
      const tried = r.candidates_tried.map((t) => `${t.exchange} ${t.ok ? 'OK' : `(${t.status_code ?? 'err'})`}`).join(', ');
      if (r.status === 'found' && r.found_exchange && r.found_exchange !== c.gurufocus_exchange) {
        const ok = await dialog.confirm(
          `${r.ticker} resolved on ${r.found_exchange} (current: ${c.gurufocus_exchange}).\n\nUpdate the row's exchange to ${r.found_exchange}?\n\nTried: ${tried}`,
          { confirmLabel: `Set to ${r.found_exchange}` },
        );
        if (!ok) return;
        // Reuse the inline-edit save path so validation + reload behave
        // identically. The handleSave signature takes a `Partial<Company>`.
        await handleSave(c.company_id, { gurufocus_exchange: r.found_exchange });
      } else if (r.status === 'found') {
        await dialog.alert(`${r.ticker} resolved on ${r.found_exchange} — already the row's exchange. The lookup-failed flag should clear on the next ingest tick.`);
      } else {
        await dialog.alert(`Could not find ${r.ticker} on any candidate exchange.\n\nTried: ${tried}\n\nMight be a stale ticker symbol or genuinely not in GuruFocus.`);
      }
    } catch (e) {
      await dialog.alert(`Lookup failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  async function handleDelete(id: number, name: string) {
    if (!(await dialog.confirm(`Delete "${name}"? This cannot be undone.`, { destructive: true, confirmLabel: 'Delete' }))) return;
    setError(null);
    setDeletingId(id);
    try {
      const res = await apiFetch(`${API_URL}/api/companies/${id}`, { method: 'DELETE' });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail ?? `HTTP ${res.status}`);
      }
      setCompanies((prev) => prev.filter((c) => c.company_id !== id));
    } catch (e) {
      setError(`Delete failed: ${e instanceof Error ? e.message : e}`);
    } finally {
      setDeletingId(null);
    }
  }

  const sortIcon = (field: SortField) => {
    if (sortField !== field) return '';
    return sortDir === 'asc' ? ' \u25B4' : ' \u25BE';
  };

  // Columns for the download button. Mirrors the visible data columns
  // (ID, Name, Ticker, Exchange, Country, Memberships) and skips the
  // Actions column (UI-only). `universes` is joined with ` | ` so each
  // row is a single CSV/XLSX cell.
  const exportColumns = useMemo<Column<Company>[]>(() => [
    { key: 'company_id', header: 'ID', accessor: (c) => c.company_id },
    { key: 'company_name', header: 'Name', accessor: (c) => c.company_name ?? '' },
    { key: 'gurufocus_ticker', header: 'Ticker', accessor: (c) => c.gurufocus_ticker },
    { key: 'gurufocus_exchange', header: 'Exchange', accessor: (c) => c.gurufocus_exchange },
    { key: 'country', header: 'Country', accessor: (c) => c.country ?? '' },
    { key: 'universes', header: 'Memberships', accessor: (c) => (c.universes ?? []).join(' | ') },
    { key: 'gurufocus_url', header: 'GuruFocus URL', accessor: (c) => guruFocusUrl(c.gurufocus_ticker, c.gurufocus_exchange) },
  ], []);

  const thCls = 'px-3 py-3 text-left text-xs font-medium cursor-pointer select-none hover:text-white transition-colors';

  return (
    <div className="flex flex-col h-full">
      <div className="px-8 py-5 border-b border-gray-800/60 flex items-center justify-between gap-4">
        <div>
          <h1 className="text-lg font-semibold text-white">Companies</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            {loading ? <LoadingDots label="Loading" /> : `${filtered.length} of ${companies.length} companies`}
            {!loading && duplicateCount > 0 && (
              <>
                {' · '}
                <button
                  onClick={() => setFilterDupes(!filterDupes)}
                  className={`underline-offset-2 hover:underline transition-colors ${
                    filterDupes ? 'text-rose-400' : 'text-rose-400/80 hover:text-rose-400'
                  }`}
                  title={filterDupes ? 'Click to clear duplicates filter' : 'Click to show only duplicate entries'}
                >
                  {duplicateCount} duplicate{duplicateCount === 1 ? '' : 's'}
                </button>
              </>
            )}
          </p>
        </div>
        {isAdmin && (
          <button
            onClick={() => { setAdding(true); setEditingId(null); }}
            className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
          >
            + Add company
          </button>
        )}
      </div>

      <div className="px-8 py-3 border-b border-gray-800/60 flex items-center gap-3 flex-wrap">
        <input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search name, ticker, exchange..."
          className="bg-[#151821] border border-gray-800/60 rounded-lg px-3 py-2 text-sm text-white w-72 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 placeholder-gray-600 transition-colors"
        />
        <MultiSelectFilter
          label="Exchanges"
          options={exchangeOptions}
          selected={filterExchange}
          onChange={setFilterExchange}
          combineMode="OR"
        />
        <MultiSelectFilter
          label="Countries"
          options={countryOptions}
          selected={filterCountry}
          onChange={setFilterCountry}
          combineMode="OR"
        />
        <MultiSelectFilter
          label="Universes"
          options={universeOptions}
          selected={filterUniverse}
          onChange={setFilterUniverse}
          combineMode="AND"
        />
        <button
          onClick={() => setFilterDupes(!filterDupes)}
          className={`px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
            filterDupes
              ? 'bg-rose-500/20 border border-rose-500/40 text-rose-400'
              : 'bg-[#151821] border border-gray-800/60 text-gray-400 hover:text-white'
          }`}
        >
          Duplicates
        </button>
        {(search || filterExchange.length > 0 || filterCountry.length > 0 || filterUniverse.length > 0 || filterDupes) && (
          <button
            onClick={() => { setSearch(''); setFilterExchange([]); setFilterCountry([]); setFilterUniverse([]); setFilterDupes(false); }}
            className="text-sm text-gray-500 hover:text-white transition-colors"
          >
            Clear filters
          </button>
        )}
        <div className="ml-auto">
          <TableDownloadButton
            rows={filtered}
            columns={exportColumns}
            filename="companies"
            title={`Download ${filtered.length} companies as CSV / XLSX`}
          />
        </div>
      </div>

      {error && (
        <div className="mx-8 mt-4 px-4 py-3 text-sm text-rose-400 bg-rose-500/10 border border-rose-500/20 rounded-lg flex items-center justify-between">
          <span>{error}</span>
          <button onClick={() => setError(null)} className="text-gray-500 hover:text-white ml-3 text-xs">Dismiss</button>
        </div>
      )}

      <div className="flex-1 overflow-auto px-8 py-4">
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-800/60 text-gray-500">
                <th className="px-4 py-3 text-left text-xs font-medium w-12">ID</th>
                <th className={thCls} onClick={() => handleSort('company_name')}>
                  <span className="flex items-center gap-2">
                    Name{sortIcon('company_name')}
                    {!loading && (
                      duplicateNames.size > 0 ? (
                        <span className="px-1.5 py-0.5 text-[10px] font-medium bg-amber-500/15 text-amber-400 border border-amber-500/25 rounded">
                          {duplicateNames.size} dupe{duplicateNames.size > 1 ? 's' : ''}
                        </span>
                      ) : (
                        <span className="px-1.5 py-0.5 text-[10px] font-medium bg-emerald-500/15 text-emerald-400 border border-emerald-500/25 rounded">
                          no dupes
                        </span>
                      )
                    )}
                  </span>
                </th>
                <th className={`${thCls} w-24`} onClick={() => handleSort('gurufocus_ticker')}>Ticker{sortIcon('gurufocus_ticker')}</th>
                <th className={`${thCls} w-24`} onClick={() => handleSort('gurufocus_exchange')}>Exchange{sortIcon('gurufocus_exchange')}</th>
                <th className={`${thCls} w-32`} onClick={() => handleSort('country')}>Country{sortIcon('country')}</th>
                <th className="px-3 py-3 text-left text-xs font-medium">Memberships</th>
                <th className="px-3 py-3 text-left text-xs font-medium w-28">Actions</th>
              </tr>
            </thead>
            <tbody>
              {adding && (
                <AddRow
                  exchangeOptions={exchangeOptions}
                  onAdd={handleAdd}
                  onCancel={() => setAdding(false)}
                />
              )}
              {loading && (
                <tr>
                  <td colSpan={7} className="py-14 text-center">
                    <span className="inline-flex items-center gap-2.5 text-gray-500 text-sm">
                      <Spinner size={14} />
                      <span>Loading companies…</span>
                    </span>
                  </td>
                </tr>
              )}
              {filtered.map((c) =>
                editingId === c.company_id ? (
                  <EditRow
                    key={c.company_id}
                    company={c}
                    exchangeOptions={exchangeOptions}
                    onSave={(updated) => handleSave(c.company_id, updated)}
                    onCancel={() => setEditingId(null)}
                  />
                ) : (
                  <tr key={c.company_id} className="border-b border-gray-800/30 hover:bg-white/[0.02] transition-colors group">
                    <td className="px-4 py-2.5 text-gray-600 text-xs">{c.company_id}</td>
                    <td className={`px-3 py-2.5 font-medium ${c.delisted_at ? 'text-gray-500' : 'text-gray-200'}`}>
                      <span className={c.delisted_at ? 'line-through' : ''}>{c.company_name ?? '—'}</span>
                      {c.delisted_at && (
                        <span
                          className="ml-2 px-1.5 py-0.5 text-[10px] font-medium bg-rose-500/15 text-rose-300 border border-rose-500/25 rounded"
                          title={`Marked delisted on ${new Date(c.delisted_at).toLocaleString()} — GuruFocus returned no fetchable data. Excluded from backtests.`}
                        >
                          DELISTED
                        </span>
                      )}
                      {c.gurufocus_lookup_failed_at && !c.delisted_at && (
                        <button
                          type="button"
                          onClick={() => void findCorrectExchange(c)}
                          className="ml-2 px-1.5 py-0.5 text-[10px] font-medium bg-rose-500/15 text-rose-300 border border-rose-500/25 rounded hover:bg-rose-500/25 hover:text-rose-200 transition-colors cursor-pointer"
                          title={`GuruFocus returned "Stock not found" on the primary exchange + every fallback as of ${new Date(c.gurufocus_lookup_failed_at).toLocaleString()}. Likely the exchange on this row is wrong. Click to probe GuruFocus for the correct exchange.`}
                        >
                          GF LOOKUP
                        </button>
                      )}
                      {c.company_name && duplicateNames.has(c.company_name.toLowerCase().trim()) && (
                        <span className="ml-2 px-1.5 py-0.5 text-[10px] font-medium bg-amber-500/15 text-amber-400 border border-amber-500/25 rounded" title="Duplicate company name">
                          DUPE
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-2.5">
                      <a
                        href={guruFocusUrl(c.gurufocus_ticker, c.gurufocus_exchange)}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-indigo-400 hover:text-indigo-300 hover:underline transition-colors"
                      >
                        {c.gurufocus_ticker}
                      </a>
                    </td>
                    <td className="px-3 py-2.5 text-gray-400">{c.gurufocus_exchange}</td>
                    <td className="px-3 py-2.5 text-gray-400">{c.country ?? '—'}</td>
                    <td className="px-3 py-2.5">
                      {(c.universes ?? []).length === 0 ? (
                        membershipsLoading ? (
                          <Spinner size={10} className="h-2.5 w-2.5 text-gray-600" />
                        ) : (
                          <span className="text-xs text-gray-600">—</span>
                        )
                      ) : (
                        <div className="flex flex-wrap gap-1">
                          {c.universes.map((u) => (
                            <button
                              key={u}
                              onClick={() => setFilterUniverse((cur) => (cur.includes(u) ? cur.filter((x) => x !== u) : [...cur, u]))}
                              style={universeChipStyle(u)}
                              title={`Filter by ${u}`}
                              className="px-1.5 py-0.5 rounded text-[10px] font-medium border hover:brightness-125 transition"
                            >
                              {u}
                            </button>
                          ))}
                        </div>
                      )}
                    </td>
                    <td className="px-3 py-2.5">
                      {isAdmin && (
                        deletingId === c.company_id ? (
                          <span className="inline-flex items-center gap-1.5 text-xs text-rose-400">
                            <Spinner size={12} className="h-3 w-3 text-rose-400" />
                            Deleting…
                          </span>
                        ) : (
                          <div className="flex gap-1.5 opacity-0 group-hover:opacity-100 transition-opacity">
                            <button
                              onClick={() => { setEditingId(c.company_id); setAdding(false); }}
                              disabled={deletingId !== null}
                              className="px-2.5 py-1 rounded-lg text-xs text-gray-400 hover:text-white hover:bg-white/5 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                            >
                              Edit
                            </button>
                            <button
                              onClick={() => handleDelete(c.company_id, c.company_name ?? c.gurufocus_ticker)}
                              disabled={deletingId !== null}
                              className="px-2.5 py-1 rounded-lg text-xs text-gray-600 hover:text-rose-400 hover:bg-rose-500/10 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                            >
                              Delete
                            </button>
                          </div>
                        )
                      )}
                    </td>
                  </tr>
                ),
              )}
            </tbody>
          </table>
        </div>
        {!loading && filtered.length === 0 && (
          <p className="text-center text-gray-500 text-sm py-12">
            {companies.length === 0 ? 'No companies in database.' : 'No companies match your filters.'}
          </p>
        )}
      </div>

      {pendingAdd && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
          <div className="bg-[#151821] border border-gray-800/60 rounded-xl p-6 max-w-lg w-full mx-4 shadow-xl">
            <h2 className="text-base font-semibold text-white mb-2">Verify GuruFocus listing</h2>
            <p className="text-sm text-gray-400 leading-relaxed mb-4">
              Open the URL below to confirm it points to the right company. The
              ticker and exchange combination must match GuruFocus exactly,
              otherwise no price data will be available for this company.
            </p>
            <div className="bg-[#0f1117] border border-gray-800/60 rounded-lg p-3 mb-4 space-y-1.5 text-sm">
              <div className="flex justify-between gap-4">
                <span className="text-gray-500">Name</span>
                <span className="text-gray-200 font-medium text-right">{pendingAdd.company_name}</span>
              </div>
              <div className="flex justify-between gap-4">
                <span className="text-gray-500">Ticker</span>
                <span className="font-mono text-gray-200">{pendingAdd.gurufocus_ticker}</span>
              </div>
              <div className="flex justify-between gap-4">
                <span className="text-gray-500">Exchange</span>
                <span className="font-mono text-gray-200">{pendingAdd.gurufocus_exchange}</span>
              </div>
              <div className="pt-2 mt-2 border-t border-gray-800/60">
                <a
                  href={guruFocusUrl(pendingAdd.gurufocus_ticker, pendingAdd.gurufocus_exchange)}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-indigo-400 hover:text-indigo-300 hover:underline break-all transition-colors"
                >
                  {guruFocusUrl(pendingAdd.gurufocus_ticker, pendingAdd.gurufocus_exchange)}
                </a>
              </div>
            </div>
            <p className="text-sm text-gray-300 mb-4">
              Is this the company you mean to add?
            </p>
            <div className="flex justify-end gap-2">
              <button
                onClick={() => setPendingAdd(null)}
                disabled={confirming}
                className="px-4 py-2 rounded-lg text-sm font-medium text-gray-400 hover:text-white hover:bg-white/5 transition-colors disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                onClick={confirmAdd}
                disabled={confirming}
                className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 inline-flex items-center gap-2"
              >
                {confirming && <Spinner size={14} className="h-3.5 w-3.5 text-white" />}
                {confirming ? 'Adding…' : 'Yes, add this company'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
