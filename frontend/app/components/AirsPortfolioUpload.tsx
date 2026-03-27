'use client';

import { useRef, useState, useEffect, useCallback } from 'react';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

// ─── Types ────────────────────────────────────────────────────────────────────

type CompanyOption = { company_id: number; label: string };

type EditableHolding = {
  holding_name: string;
  mv_eur: number | null;
  weight_pct: string;
  currency: string;
  include: boolean;
  weight_source: string;
  match_company_id: number | null;
  match_label: string;
  match_score: number;
};

type Portfolio = {
  portfolio_id: number;
  portfolio_name: string;
  target_date: string | null;
  published_at: string | null;
};

type WeightRow = {
  company_id: number;
  weight_value: number;
  company_name: string | null;
  primary_ticker: string | null;
  primary_exchange: string | null;
};

type EditWeightRow = WeightRow & { weight_pct: string };

// ─── Helpers ──────────────────────────────────────────────────────────────────

function today(): string {
  return new Date().toISOString().slice(0, 10);
}

function scoreColor(score: number): string {
  if (score >= 0.8) return 'text-green-400';
  if (score >= 0.55) return 'text-yellow-400';
  return 'text-red-400';
}

function weightSum(rows: EditableHolding[]): number {
  return rows
    .filter((r) => r.include)
    .reduce((s, r) => s + (parseFloat(r.weight_pct) || 0), 0);
}

// ─── Drop zone ────────────────────────────────────────────────────────────────

function DropZone({ onFile, disabled }: { onFile: (f: File) => void; disabled: boolean }) {
  const [dragging, setDragging] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  return (
    <div
      onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
      onDragLeave={() => setDragging(false)}
      onDrop={(e) => {
        e.preventDefault();
        setDragging(false);
        const f = e.dataTransfer.files[0];
        if (f) onFile(f);
      }}
      onClick={() => !disabled && inputRef.current?.click()}
      className={`border-2 border-dashed rounded px-6 py-10 text-center cursor-pointer transition-colors ${
        dragging ? 'border-gray-400 bg-gray-800' : 'border-gray-700 hover:border-gray-500'
      } ${disabled ? 'opacity-40 cursor-not-allowed' : ''}`}
    >
      <input
        ref={inputRef}
        type="file"
        accept=".xls,.xlsx"
        className="hidden"
        onChange={(e) => { const f = e.target.files?.[0]; if (f) onFile(f); }}
      />
      <p className="font-mono text-sm text-gray-400">
        Drag & drop AIRS Excel (.xls / .xlsx) or click to select
      </p>
      <p className="font-mono text-xs text-gray-600 mt-1">
        Expected columns: Fondsomschrijving, Huidige waarde EUR
      </p>
    </div>
  );
}

// ─── Review table ─────────────────────────────────────────────────────────────

function ReviewTable({
  holdings,
  companies,
  onChange,
}: {
  holdings: EditableHolding[];
  companies: CompanyOption[];
  onChange: (rows: EditableHolding[]) => void;
}) {
  function update(i: number, patch: Partial<EditableHolding>) {
    const next = holdings.map((r, idx) => (idx === i ? { ...r, ...patch } : r));
    onChange(next);
  }

  const sum = weightSum(holdings);
  const included = holdings.filter((r) => r.include).length;

  return (
    <div>
      <div className="flex items-center gap-4 mb-2">
        <span className="font-mono text-xs text-gray-500">
          {included} included · weight sum:{' '}
          <span className={Math.abs(sum - 100) < 0.5 ? 'text-green-400' : 'text-yellow-400'}>
            {sum.toFixed(2)}%
          </span>
        </span>
      </div>
      <div className="overflow-x-auto border border-gray-800 rounded">
        <table className="w-full text-xs font-mono">
          <thead>
            <tr className="border-b border-gray-800 text-gray-600">
              <th className="px-2 py-1 text-left w-8">✓</th>
              <th className="px-2 py-1 text-left">Holding name</th>
              <th className="px-2 py-1 text-right w-24">Weight %</th>
              <th className="px-2 py-1 text-left">Matched company</th>
              <th className="px-2 py-1 text-right w-16">Score</th>
              <th className="px-2 py-1 text-right w-28">Mkt val EUR</th>
            </tr>
          </thead>
          <tbody>
            {holdings.map((row, i) => (
              <tr
                key={i}
                className={`border-b border-gray-900 ${row.include ? '' : 'opacity-40'}`}
              >
                <td className="px-2 py-1">
                  <input
                    type="checkbox"
                    checked={row.include}
                    onChange={(e) => update(i, { include: e.target.checked })}
                    className="accent-gray-400"
                  />
                </td>
                <td className="px-2 py-1 text-gray-300">{row.holding_name}</td>
                <td className="px-2 py-1">
                  <input
                    type="number"
                    value={row.weight_pct}
                    step="0.01"
                    min="0"
                    max="100"
                    onChange={(e) => update(i, { weight_pct: e.target.value })}
                    className="w-full bg-gray-900 border border-gray-700 rounded px-1 py-0.5 text-right text-white focus:outline-none focus:border-gray-500"
                  />
                </td>
                <td className="px-2 py-1">
                  <select
                    value={row.match_company_id ?? ''}
                    onChange={(e) => {
                      const val = e.target.value;
                      const cid = val === '' ? null : parseInt(val, 10);
                      const opt = companies.find((c) => c.company_id === cid);
                      update(i, {
                        match_company_id: cid,
                        match_label: opt?.label ?? '',
                      });
                    }}
                    className="w-full bg-gray-900 border border-gray-700 rounded px-1 py-0.5 text-gray-300 focus:outline-none focus:border-gray-500"
                  >
                    <option value="">— unmatched —</option>
                    {companies.map((c) => (
                      <option key={c.company_id} value={c.company_id}>
                        {c.label}
                      </option>
                    ))}
                  </select>
                </td>
                <td className={`px-2 py-1 text-right ${scoreColor(row.match_score)}`}>
                  {row.match_score > 0 ? (row.match_score * 100).toFixed(0) + '%' : '—'}
                </td>
                <td className="px-2 py-1 text-right text-gray-500">
                  {row.mv_eur != null ? row.mv_eur.toLocaleString('nl-NL', { maximumFractionDigits: 0 }) : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Existing portfolios ───────────────────────────────────────────────────────

function ExistingPortfolios({
  portfolios,
  onRefresh,
}: {
  portfolios: Portfolio[];
  onRefresh: () => void;
}) {
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [weights, setWeights] = useState<Record<number, WeightRow[]>>({});
  const [editWeights, setEditWeights] = useState<Record<number, EditWeightRow[]>>({});
  const [loadingId, setLoadingId] = useState<number | null>(null);
  const [deletingId, setDeletingId] = useState<number | null>(null);
  const [savingId, setSavingId] = useState<number | null>(null);

  async function handleExpand(id: number) {
    if (expandedId === id) { setExpandedId(null); return; }
    setExpandedId(id);
    if (weights[id]) return;
    setLoadingId(id);
    const res = await fetch(`${API_URL}/api/portfolios/${id}/weights`);
    const data: WeightRow[] = await res.json();
    setWeights((p) => ({ ...p, [id]: data }));
    setEditWeights((p) => ({
      ...p,
      [id]: data.map((r) => ({ ...r, weight_pct: (r.weight_value * 100).toFixed(4) })),
    }));
    setLoadingId(null);
  }

  async function handleDelete(id: number) {
    if (!confirm('Delete this portfolio and all its weights?')) return;
    setDeletingId(id);
    await fetch(`${API_URL}/api/portfolios/${id}`, { method: 'DELETE' });
    setDeletingId(null);
    setExpandedId(null);
    onRefresh();
  }

  async function handleSaveWeights(id: number) {
    const rows = editWeights[id] ?? [];
    const validRows = rows.filter((r) => parseFloat(r.weight_pct) > 0);
    const total = validRows.reduce((s, r) => s + parseFloat(r.weight_pct), 0);
    const wPayload = validRows.map((r) => ({
      company_id: r.company_id,
      weight: parseFloat(r.weight_pct) / 100,
    }));
    setSavingId(id);
    await fetch(`${API_URL}/api/portfolios/${id}/weights`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ weights: wPayload, normalize: Math.abs(total - 100) > 0.5 }),
    });
    // Reload weights
    const res = await fetch(`${API_URL}/api/portfolios/${id}/weights`);
    const data: WeightRow[] = await res.json();
    setWeights((p) => ({ ...p, [id]: data }));
    setEditWeights((p) => ({
      ...p,
      [id]: data.map((r) => ({ ...r, weight_pct: (r.weight_value * 100).toFixed(4) })),
    }));
    setSavingId(null);
  }

  if (portfolios.length === 0) {
    return <p className="font-mono text-xs text-gray-600">No portfolios yet.</p>;
  }

  return (
    <div className="space-y-2">
      {portfolios.map((p) => {
        const isExpanded = expandedId === p.portfolio_id;
        const wRows = editWeights[p.portfolio_id] ?? [];
        return (
          <div key={p.portfolio_id} className="border border-gray-800 rounded">
            <div className="flex items-center gap-3 px-3 py-2">
              <button
                onClick={() => handleExpand(p.portfolio_id)}
                className="text-gray-400 w-4 shrink-0"
              >
                {isExpanded ? '▾' : '▸'}
              </button>
              <span className="font-mono text-sm text-white flex-1">{p.portfolio_name}</span>
              {p.target_date && (
                <span className="font-mono text-xs text-gray-500">
                  {new Date(p.target_date).toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' })}
                </span>
              )}
              <button
                onClick={() => handleDelete(p.portfolio_id)}
                disabled={deletingId === p.portfolio_id}
                className="px-2 py-0.5 rounded text-xs font-mono text-red-500 hover:text-red-300 hover:bg-gray-800 transition-colors disabled:opacity-40"
              >
                {deletingId === p.portfolio_id ? 'Deleting...' : 'Delete'}
              </button>
            </div>

            {isExpanded && (
              <div className="border-t border-gray-800 px-3 py-2">
                {loadingId === p.portfolio_id ? (
                  <p className="font-mono text-xs text-gray-500">Loading...</p>
                ) : (
                  <>
                    <div className="overflow-x-auto">
                      <table className="w-full text-xs font-mono mb-2">
                        <thead>
                          <tr className="border-b border-gray-800 text-gray-600">
                            <th className="px-2 py-1 text-left">Company</th>
                            <th className="px-2 py-1 text-left">Ticker</th>
                            <th className="px-2 py-1 text-left">Exchange</th>
                            <th className="px-2 py-1 text-right w-28">Weight %</th>
                          </tr>
                        </thead>
                        <tbody>
                          {wRows.map((r, i) => (
                            <tr key={r.company_id} className="border-b border-gray-900">
                              <td className="px-2 py-0.5 text-gray-300">{r.company_name ?? '—'}</td>
                              <td className="px-2 py-0.5 text-gray-400">{r.primary_ticker ?? '—'}</td>
                              <td className="px-2 py-0.5 text-gray-500">{r.primary_exchange ?? '—'}</td>
                              <td className="px-2 py-0.5">
                                <input
                                  type="number"
                                  value={r.weight_pct}
                                  step="0.01"
                                  min="0"
                                  max="100"
                                  onChange={(e) => {
                                    const next = wRows.map((row, j) =>
                                      j === i ? { ...row, weight_pct: e.target.value } : row
                                    );
                                    setEditWeights((prev) => ({ ...prev, [p.portfolio_id]: next }));
                                  }}
                                  className="w-full bg-gray-900 border border-gray-700 rounded px-1 py-0.5 text-right text-white focus:outline-none focus:border-gray-500"
                                />
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className="font-mono text-xs text-gray-600">
                        Sum: {wRows.reduce((s, r) => s + parseFloat(r.weight_pct || '0'), 0).toFixed(2)}%
                      </span>
                      <button
                        onClick={() => handleSaveWeights(p.portfolio_id)}
                        disabled={savingId === p.portfolio_id}
                        className="px-3 py-1 rounded text-xs font-mono bg-gray-700 hover:bg-gray-600 disabled:opacity-50 text-white transition-colors"
                      >
                        {savingId === p.portfolio_id ? 'Saving...' : 'Save weights'}
                      </button>
                    </div>
                  </>
                )}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ─── Main component ───────────────────────────────────────────────────────────

export default function AirsPortfolioUpload() {
  const [parsing, setParsing] = useState(false);
  const [parseError, setParseError] = useState<string | null>(null);
  const [holdings, setHoldings] = useState<EditableHolding[] | null>(null);
  const [companies, setCompanies] = useState<CompanyOption[]>([]);
  const [weightSource, setWeightSource] = useState<string>('');

  const [portfolioName, setPortfolioName] = useState('');
  const [targetDate, setTargetDate] = useState(today());
  const [publishedAt, setPublishedAt] = useState(today());
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saveSuccess, setSaveSuccess] = useState(false);

  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [loadingPortfolios, setLoadingPortfolios] = useState(true);

  const loadPortfolios = useCallback(async () => {
    setLoadingPortfolios(true);
    try {
      const res = await fetch(`${API_URL}/api/portfolios`);
      setPortfolios(await res.json());
    } catch {
      //
    }
    setLoadingPortfolios(false);
  }, []);

  useEffect(() => { loadPortfolios(); }, [loadPortfolios]);

  async function handleFile(file: File) {
    setParsing(true);
    setParseError(null);
    setHoldings(null);
    setSaveSuccess(false);

    const form = new FormData();
    form.append('file', file);
    try {
      const res = await fetch(`${API_URL}/api/portfolios/parse`, { method: 'POST', body: form });
      if (!res.ok) {
        const err = await res.json();
        setParseError(err.detail ?? 'Parse failed');
        setParsing(false);
        return;
      }
      const data = await res.json();
      setCompanies(data.companies ?? []);
      const ws: string = data.holdings?.[0]?.weight_source ?? 'computed';
      setWeightSource(ws);
      const rows: EditableHolding[] = (data.holdings ?? []).map((h: any) => ({
        holding_name: h.holding_name,
        mv_eur: h.mv_eur,
        weight_pct: h.weight != null ? (h.weight * 100).toFixed(4) : '',
        currency: h.currency ?? '',
        include: h.include,
        weight_source: h.weight_source,
        match_company_id: h.match_company_id,
        match_label: h.match_label ?? '',
        match_score: h.match_score ?? 0,
      }));
      setHoldings(rows);
      // Auto-fill portfolio name from file
      const base = file.name.replace(/\.[^.]+$/, '').replace(/_/g, ' ');
      setPortfolioName(base);
    } catch (e) {
      setParseError(String(e));
    }
    setParsing(false);
  }

  async function handleSave() {
    if (!holdings) return;
    const included = holdings.filter(
      (r) => r.include && r.match_company_id != null && parseFloat(r.weight_pct) > 0
    );
    if (included.length === 0) {
      setSaveError('No valid included rows with a matched company and weight > 0.');
      return;
    }

    setSaving(true);
    setSaveError(null);
    setSaveSuccess(false);

    const weights = included.map((r) => ({
      company_id: r.match_company_id!,
      weight: parseFloat(r.weight_pct) / 100,
    }));

    try {
      const res = await fetch(`${API_URL}/api/portfolios`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          portfolio_name: portfolioName,
          target_date: targetDate,
          published_at: publishedAt,
          weights,
          normalize: true,
        }),
      });
      if (!res.ok) {
        const err = await res.json();
        setSaveError(err.detail ?? 'Save failed');
      } else {
        setSaveSuccess(true);
        setHoldings(null);
        loadPortfolios();
      }
    } catch (e) {
      setSaveError(String(e));
    }
    setSaving(false);
  }

  const unmatched = holdings?.filter((r) => r.include && r.match_company_id == null).length ?? 0;

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-800">
        <h1 className="font-mono text-base font-bold text-white">AIRS Portfolio Upload</h1>
        <p className="text-xs text-gray-500 font-mono mt-0.5">
          Upload an AIRS Excel export to create a portfolio in the database.
        </p>
      </div>

      <div className="flex-1 overflow-auto px-6 py-4 space-y-8">

        {/* ── Upload section ── */}
        <section>
          <h2 className="font-mono text-sm font-semibold text-gray-300 mb-3">Upload & review</h2>

          <DropZone onFile={handleFile} disabled={parsing} />

          {parsing && (
            <p className="font-mono text-xs text-gray-500 mt-3">Parsing file…</p>
          )}
          {parseError && (
            <p className="font-mono text-xs text-red-400 mt-3">{parseError}</p>
          )}

          {holdings && (
            <div className="mt-4 space-y-4">
              {weightSource === 'computed' && (
                <p className="font-mono text-xs text-gray-500">
                  Weight computed from market value (Weging column not found or empty).
                </p>
              )}
              {unmatched > 0 && (
                <p className="font-mono text-xs text-yellow-400">
                  {unmatched} included holding(s) have no matched company — they will be skipped on save.
                </p>
              )}

              <ReviewTable
                holdings={holdings}
                companies={companies}
                onChange={setHoldings}
              />

              {/* Save form */}
              <div className="border border-gray-800 rounded px-4 py-3 space-y-3">
                <p className="font-mono text-xs text-gray-500 font-semibold">Save as portfolio</p>
                <div className="grid grid-cols-3 gap-3">
                  <div>
                    <label className="block font-mono text-xs text-gray-500 mb-1">Portfolio name</label>
                    <input
                      value={portfolioName}
                      onChange={(e) => setPortfolioName(e.target.value)}
                      className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs font-mono text-white focus:outline-none focus:border-gray-500"
                    />
                  </div>
                  <div>
                    <label className="block font-mono text-xs text-gray-500 mb-1">Target date</label>
                    <input
                      type="date"
                      value={targetDate}
                      onChange={(e) => setTargetDate(e.target.value)}
                      className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs font-mono text-white focus:outline-none focus:border-gray-500"
                    />
                  </div>
                  <div>
                    <label className="block font-mono text-xs text-gray-500 mb-1">Published at</label>
                    <input
                      type="date"
                      value={publishedAt}
                      onChange={(e) => setPublishedAt(e.target.value)}
                      className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs font-mono text-white focus:outline-none focus:border-gray-500"
                    />
                  </div>
                </div>
                {saveError && <p className="font-mono text-xs text-red-400">{saveError}</p>}
                {saveSuccess && <p className="font-mono text-xs text-green-400">Portfolio saved successfully.</p>}
                <button
                  onClick={handleSave}
                  disabled={saving || !portfolioName.trim()}
                  className="px-4 py-1.5 rounded text-xs font-mono bg-gray-700 hover:bg-gray-600 disabled:opacity-50 text-white transition-colors"
                >
                  {saving ? 'Saving…' : 'Save portfolio'}
                </button>
              </div>
            </div>
          )}
        </section>

        {/* ── Existing portfolios section ── */}
        <section>
          <div className="flex items-center gap-3 mb-3">
            <h2 className="font-mono text-sm font-semibold text-gray-300">Existing portfolios</h2>
            <button
              onClick={loadPortfolios}
              className="text-xs font-mono text-gray-600 hover:text-gray-400 transition-colors"
            >
              ↻ refresh
            </button>
          </div>
          {loadingPortfolios ? (
            <p className="font-mono text-xs text-gray-500">Loading…</p>
          ) : (
            <ExistingPortfolios portfolios={portfolios} onRefresh={loadPortfolios} />
          )}
        </section>

      </div>
    </div>
  );
}
