'use client';

import { useState, useEffect, useCallback, useMemo, useRef } from 'react';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type Company = {
  company_id: number;
  company_name: string | null;
  primary_ticker: string;
  primary_exchange: string;
  longequity_ticker: string | null;
  country: string | null;
  sector: string | null;
};

type SortField = 'company_name' | 'primary_ticker' | 'primary_exchange' | 'country' | 'sector';
type SortDir = 'asc' | 'desc';

function guruFocusUrl(ticker: string, exchange: string): string {
  const USA = new Set(['NYSE', 'NASDAQ', 'US', 'AMEX']);
  const t = ticker.toUpperCase();
  const e = exchange.toUpperCase();
  if (USA.has(e)) return `https://www.gurufocus.com/stock/${t}/summary`;
  return `https://www.gurufocus.com/stock/${e}:${t}/summary`;
}

const inputCls = 'w-full bg-[#0f1117] border border-gray-700 rounded-lg px-2.5 py-1.5 text-sm text-white focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors';
const inputAddCls = 'w-full bg-[#0f1117] border border-emerald-800/50 rounded-lg px-2.5 py-1.5 text-sm text-white focus:outline-none focus:border-emerald-500 focus:ring-1 focus:ring-emerald-500/30 transition-colors';

// ─── Inline edit row ──────────────────────────────────────────────────────────

function EditRow({
  company,
  exchangeOptions,
  countryOptions,
  sectorOptions,
  onSave,
  onCancel,
}: {
  company: Company;
  exchangeOptions: string[];
  countryOptions: string[];
  sectorOptions: string[];
  onSave: (updated: Partial<Company>) => Promise<void>;
  onCancel: () => void;
}) {
  const [name, setName] = useState(company.company_name ?? '');
  const [ticker, setTicker] = useState(company.primary_ticker);
  const [exchange, setExchange] = useState(company.primary_exchange);
  const [country, setCountry] = useState(company.country ?? '');
  const [sector, setSector] = useState(company.sector ?? '');
  const [saving, setSaving] = useState(false);

  async function handleSave() {
    setSaving(true);
    await onSave({ company_name: name, primary_ticker: ticker, primary_exchange: exchange, country, sector });
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
      <td className="px-3 py-2 text-sm text-gray-500">{company.longequity_ticker ?? '—'}</td>
      <td className="px-3 py-2">
        <input list="edit-country" value={country} onChange={(e) => setCountry(e.target.value)} className={inputCls} />
        <datalist id="edit-country">{countryOptions.map((o) => <option key={o} value={o} />)}</datalist>
      </td>
      <td className="px-3 py-2">
        <input list="edit-sector" value={sector} onChange={(e) => setSector(e.target.value)} className={inputCls} />
        <datalist id="edit-sector">{sectorOptions.map((o) => <option key={o} value={o} />)}</datalist>
      </td>
      <td className="px-3 py-2">
        <div className="flex gap-1.5">
          <button onClick={handleSave} disabled={saving} className="px-3 py-1.5 rounded-lg text-xs font-medium bg-indigo-600 hover:bg-indigo-500 text-white disabled:opacity-50 transition-colors">
            {saving ? '...' : 'Save'}
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

function AddRow({
  exchangeOptions,
  countryOptions,
  sectorOptions,
  onAdd,
  onCancel,
}: {
  exchangeOptions: string[];
  countryOptions: string[];
  sectorOptions: string[];
  onAdd: (c: { company_name: string; primary_ticker: string; primary_exchange: string; country: string; sector: string }) => Promise<void>;
  onCancel: () => void;
}) {
  const [name, setName] = useState('');
  const [ticker, setTicker] = useState('');
  const [exchange, setExchange] = useState('');
  const [country, setCountry] = useState('');
  const [sector, setSector] = useState('');
  const [saving, setSaving] = useState(false);
  const nameRef = useRef<HTMLInputElement>(null);

  useEffect(() => { nameRef.current?.focus(); }, []);

  async function handleAdd() {
    if (!name.trim() || !ticker.trim() || !exchange.trim()) return;
    setSaving(true);
    await onAdd({ company_name: name.trim(), primary_ticker: ticker.trim(), primary_exchange: exchange.trim(), country: country.trim(), sector: sector.trim() });
    setSaving(false);
  }

  return (
    <tr className="border-b border-emerald-800/20 bg-emerald-500/5">
      <td className="px-4 py-2 text-sm text-gray-600">new</td>
      <td className="px-3 py-2"><input ref={nameRef} value={name} onChange={(e) => setName(e.target.value)} placeholder="Company name" className={inputAddCls} /></td>
      <td className="px-3 py-2"><input value={ticker} onChange={(e) => setTicker(e.target.value)} placeholder="TICKER" className={inputAddCls} /></td>
      <td className="px-3 py-2">
        <input list="add-exchange" value={exchange} onChange={(e) => setExchange(e.target.value)} placeholder="EXCHANGE" className={inputAddCls} />
        <datalist id="add-exchange">{exchangeOptions.map((o) => <option key={o} value={o} />)}</datalist>
      </td>
      <td className="px-3 py-2 text-sm text-gray-600">—</td>
      <td className="px-3 py-2">
        <input list="add-country" value={country} onChange={(e) => setCountry(e.target.value)} placeholder="Country" className={inputAddCls} />
        <datalist id="add-country">{countryOptions.map((o) => <option key={o} value={o} />)}</datalist>
      </td>
      <td className="px-3 py-2">
        <input list="add-sector" value={sector} onChange={(e) => setSector(e.target.value)} placeholder="Sector" className={inputAddCls} />
        <datalist id="add-sector">{sectorOptions.map((o) => <option key={o} value={o} />)}</datalist>
      </td>
      <td className="px-3 py-2">
        <div className="flex gap-1.5">
          <button onClick={handleAdd} disabled={saving || !name.trim() || !ticker.trim() || !exchange.trim()} className="px-3 py-1.5 rounded-lg text-xs font-medium bg-emerald-600 hover:bg-emerald-500 text-white disabled:opacity-50 transition-colors">
            {saving ? '...' : 'Add'}
          </button>
          <button onClick={onCancel} className="px-3 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition-colors">
            Cancel
          </button>
        </div>
      </td>
    </tr>
  );
}

// ─── Main ─────────────────────────────────────────────────────────────────────

export default function CompanyManager() {
  const [companies, setCompanies] = useState<Company[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [filterExchange, setFilterExchange] = useState('');
  const [filterCountry, setFilterCountry] = useState('');
  const [filterSector, setFilterSector] = useState('');
  const [sortField, setSortField] = useState<SortField>('company_name');
  const [sortDir, setSortDir] = useState<SortDir>('asc');
  const [editingId, setEditingId] = useState<number | null>(null);
  const [adding, setAdding] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_URL}/api/companies`);
      setCompanies(await res.json());
    } catch {
      setError('Failed to load companies');
    }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const exchangeOptions = useMemo(() => {
    const s = new Set(companies.map((c) => c.primary_exchange).filter(Boolean));
    return [...s].sort();
  }, [companies]);

  const countryOptions = useMemo(() => {
    const s = new Set(companies.map((c) => c.country).filter((v): v is string => !!v?.trim()));
    return [...s].sort();
  }, [companies]);

  const sectorOptions = useMemo(() => {
    const s = new Set(companies.map((c) => c.sector).filter((v): v is string => !!v?.trim()));
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
          c.primary_ticker.toLowerCase().includes(q) ||
          (c.longequity_ticker ?? '').toLowerCase().includes(q) ||
          c.primary_exchange.toLowerCase().includes(q),
      );
    }
    if (filterExchange) list = list.filter((c) => c.primary_exchange === filterExchange);
    if (filterCountry) list = list.filter((c) => c.country === filterCountry);
    if (filterSector) list = list.filter((c) => c.sector === filterSector);

    return [...list].sort((a, b) => {
      const av = (a[sortField] ?? '') as string;
      const bv = (b[sortField] ?? '') as string;
      const cmp = av.localeCompare(bv, undefined, { sensitivity: 'base' });
      return sortDir === 'asc' ? cmp : -cmp;
    });
  }, [companies, search, filterExchange, filterCountry, filterSector, sortField, sortDir]);

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
      const res = await fetch(`${API_URL}/api/companies/${id}`, {
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

  async function handleAdd(data: { company_name: string; primary_ticker: string; primary_exchange: string; country: string; sector: string }) {
    setError(null);
    try {
      const res = await fetch(`${API_URL}/api/companies`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail ?? `HTTP ${res.status}`);
      }
      setAdding(false);
      await load();
    } catch (e) {
      setError(`Add failed: ${e instanceof Error ? e.message : e}`);
    }
  }

  async function handleDelete(id: number, name: string) {
    if (!confirm(`Delete "${name}"? This cannot be undone.`)) return;
    setError(null);
    try {
      const res = await fetch(`${API_URL}/api/companies/${id}`, { method: 'DELETE' });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail ?? `HTTP ${res.status}`);
      }
      setCompanies((prev) => prev.filter((c) => c.company_id !== id));
    } catch (e) {
      setError(`Delete failed: ${e instanceof Error ? e.message : e}`);
    }
  }

  const sortIcon = (field: SortField) => {
    if (sortField !== field) return '';
    return sortDir === 'asc' ? ' \u25B4' : ' \u25BE';
  };

  const thCls = 'px-3 py-3 text-left text-xs font-medium cursor-pointer select-none hover:text-white transition-colors';

  return (
    <div className="flex flex-col h-full">
      <div className="px-8 py-5 border-b border-gray-800/60 flex items-center justify-between gap-4">
        <div>
          <h1 className="text-lg font-semibold text-white">Companies</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            {loading ? 'Loading...' : `${filtered.length} of ${companies.length} companies`}
          </p>
        </div>
        <button
          onClick={() => { setAdding(true); setEditingId(null); }}
          className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
        >
          + Add company
        </button>
      </div>

      <div className="px-8 py-3 border-b border-gray-800/60 flex items-center gap-3 flex-wrap">
        <input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search name, ticker, exchange..."
          className="bg-[#151821] border border-gray-800/60 rounded-lg px-3 py-2 text-sm text-white w-72 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 placeholder-gray-600 transition-colors"
        />
        <select
          value={filterExchange}
          onChange={(e) => setFilterExchange(e.target.value)}
          className="bg-[#151821] border border-gray-800/60 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-indigo-500 transition-colors"
        >
          <option value="">All exchanges</option>
          {exchangeOptions.map((e) => <option key={e} value={e}>{e}</option>)}
        </select>
        <select
          value={filterCountry}
          onChange={(e) => setFilterCountry(e.target.value)}
          className="bg-[#151821] border border-gray-800/60 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-indigo-500 transition-colors"
        >
          <option value="">All countries</option>
          {countryOptions.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
        <select
          value={filterSector}
          onChange={(e) => setFilterSector(e.target.value)}
          className="bg-[#151821] border border-gray-800/60 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-indigo-500 transition-colors"
        >
          <option value="">All sectors</option>
          {sectorOptions.map((s) => <option key={s} value={s}>{s}</option>)}
        </select>
        {(search || filterExchange || filterCountry || filterSector) && (
          <button
            onClick={() => { setSearch(''); setFilterExchange(''); setFilterCountry(''); setFilterSector(''); }}
            className="text-sm text-gray-500 hover:text-white transition-colors"
          >
            Clear filters
          </button>
        )}
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
                <th className={`${thCls} w-24`} onClick={() => handleSort('primary_ticker')}>Ticker{sortIcon('primary_ticker')}</th>
                <th className={`${thCls} w-24`} onClick={() => handleSort('primary_exchange')}>Exchange{sortIcon('primary_exchange')}</th>
                <th className="px-3 py-3 text-left text-xs font-medium w-24">LE Ticker</th>
                <th className={`${thCls} w-32`} onClick={() => handleSort('country')}>Country{sortIcon('country')}</th>
                <th className={`${thCls} w-32`} onClick={() => handleSort('sector')}>Sector{sortIcon('sector')}</th>
                <th className="px-3 py-3 text-left text-xs font-medium w-28">Actions</th>
              </tr>
            </thead>
            <tbody>
              {adding && (
                <AddRow
                  exchangeOptions={exchangeOptions}
                  countryOptions={countryOptions}
                  sectorOptions={sectorOptions}
                  onAdd={handleAdd}
                  onCancel={() => setAdding(false)}
                />
              )}
              {filtered.map((c) =>
                editingId === c.company_id ? (
                  <EditRow
                    key={c.company_id}
                    company={c}
                    exchangeOptions={exchangeOptions}
                    countryOptions={countryOptions}
                    sectorOptions={sectorOptions}
                    onSave={(updated) => handleSave(c.company_id, updated)}
                    onCancel={() => setEditingId(null)}
                  />
                ) : (
                  <tr key={c.company_id} className="border-b border-gray-800/30 hover:bg-white/[0.02] transition-colors group">
                    <td className="px-4 py-2.5 text-gray-600 text-xs">{c.company_id}</td>
                    <td className="px-3 py-2.5 text-gray-200 font-medium">
                      {c.company_name ?? '—'}
                      {c.company_name && duplicateNames.has(c.company_name.toLowerCase().trim()) && (
                        <span className="ml-2 px-1.5 py-0.5 text-[10px] font-medium bg-amber-500/15 text-amber-400 border border-amber-500/25 rounded" title="Duplicate company name">
                          DUPE
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-2.5">
                      <a
                        href={guruFocusUrl(c.primary_ticker, c.primary_exchange)}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-indigo-400 hover:text-indigo-300 hover:underline transition-colors"
                      >
                        {c.primary_ticker}
                      </a>
                    </td>
                    <td className="px-3 py-2.5 text-gray-400">{c.primary_exchange}</td>
                    <td className="px-3 py-2.5 text-gray-500 text-xs">{c.longequity_ticker ?? '—'}</td>
                    <td className="px-3 py-2.5 text-gray-400">{c.country ?? '—'}</td>
                    <td className="px-3 py-2.5 text-gray-400">{c.sector ?? '—'}</td>
                    <td className="px-3 py-2.5">
                      <div className="flex gap-1.5 opacity-0 group-hover:opacity-100 transition-opacity">
                        <button
                          onClick={() => { setEditingId(c.company_id); setAdding(false); }}
                          className="px-2.5 py-1 rounded-lg text-xs text-gray-400 hover:text-white hover:bg-white/5 transition-colors"
                        >
                          Edit
                        </button>
                        <button
                          onClick={() => handleDelete(c.company_id, c.company_name ?? c.primary_ticker)}
                          className="px-2.5 py-1 rounded-lg text-xs text-gray-600 hover:text-rose-400 hover:bg-rose-500/10 transition-colors"
                        >
                          Delete
                        </button>
                      </div>
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
    </div>
  );
}
