'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type ExchangeFee = {
  exchange_code: string;
  exchange_name: string | null;
  is_us: boolean | null;
  country_code: string | null;
  currency_code: string | null;
  fee_bps: number;
  is_broker_supported: boolean;
  updated_at: string | null;
};

/** Per-row editable state. Original-from-server values stay alongside
 * the editable ones so we can detect "is this row pending a save" with
 * a strict equality check (no need to track a separate dirty flag
 * per row). */
type RowState = {
  fee_bps: string;          // editable input string
  is_supported: boolean;     // editable checkbox
  server_fee_bps: number;
  server_is_supported: boolean;
  error: string | null;     // per-row error after a failed save
};

function formatPct(bps: number): string {
  if (bps === 0) return '0%';
  return `${(bps / 100).toFixed(4).replace(/\.?0+$/, '')}%`;
}

function isRowPending(s: RowState): boolean {
  const feeNum = s.fee_bps.trim() === '' ? 0 : Number(s.fee_bps);
  if (!Number.isFinite(feeNum)) return true; // invalid input counts as pending
  if (feeNum !== s.server_fee_bps) return true;
  if (s.is_supported !== s.server_is_supported) return true;
  return false;
}

function validateRow(s: RowState): string | null {
  const trimmed = s.fee_bps.trim();
  if (trimmed === '') return null; // treated as 0
  const n = Number(trimmed);
  if (!Number.isFinite(n)) return 'Fee must be a number';
  if (n < 0 || n > 10000) return 'Fee must be between 0 and 10000 bps';
  return null;
}

export default function Fees() {
  const [rows, setRows] = useState<ExchangeFee[]>([]);
  const [state, setState] = useState<Record<string, RowState>>({});
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saveOk, setSaveOk] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [showOnlyUnsupported, setShowOnlyUnsupported] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const r = await fetch(`${API_URL}/api/exchange-fees`);
      if (!r.ok) {
        setLoadError(`Failed to load (${r.status})`);
        return;
      }
      const data = (await r.json()) as ExchangeFee[];
      setRows(data);
      const next: Record<string, RowState> = {};
      for (const row of data) {
        next[row.exchange_code] = {
          fee_bps: String(row.fee_bps),
          is_supported: row.is_broker_supported,
          server_fee_bps: row.fee_bps,
          server_is_supported: row.is_broker_supported,
          error: null,
        };
      }
      setState(next);
      setLoadError(null);
    } catch (e) {
      setLoadError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  const pendingCodes = useMemo(() => {
    return Object.entries(state)
      .filter(([, s]) => isRowPending(s))
      .map(([code]) => code);
  }, [state]);
  const pendingCount = pendingCodes.length;

  const handleSave = async () => {
    if (pendingCount === 0) return;

    // Front-load validation so we don't fire partial PUTs and end up
    // with the UI half-saved.
    const errors: string[] = [];
    for (const code of pendingCodes) {
      const v = validateRow(state[code]);
      if (v) errors.push(`${code}: ${v}`);
    }
    if (errors.length > 0) {
      setSaveError(`Fix these first: ${errors.slice(0, 3).join('; ')}${errors.length > 3 ? ` (+${errors.length - 3} more)` : ''}`);
      return;
    }

    setSaving(true);
    setSaveError(null);
    setSaveOk(null);
    let okCount = 0;
    const failed: string[] = [];
    // Sequential PUTs — there are at most ~50 exchanges so the latency
    // is fine, and serial keeps error attribution cleaner than racing
    // them.
    for (const code of pendingCodes) {
      const s = state[code];
      const feeNum = s.fee_bps.trim() === '' ? 0 : Number(s.fee_bps);
      try {
        const r = await apiFetch(`${API_URL}/api/exchange-fees/${encodeURIComponent(code)}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            fee_bps: feeNum,
            is_broker_supported: s.is_supported,
          }),
        });
        if (!r.ok) {
          const body = await r.text().catch(() => '');
          failed.push(`${code}: ${r.status} ${body.slice(0, 80)}`);
          setState((prev) => ({
            ...prev,
            [code]: { ...prev[code], error: `${r.status}` },
          }));
        } else {
          okCount++;
          setState((prev) => ({
            ...prev,
            [code]: {
              ...prev[code],
              server_fee_bps: feeNum,
              server_is_supported: s.is_supported,
              fee_bps: String(feeNum),
              error: null,
            },
          }));
          setRows((rs) =>
            rs.map((row) =>
              row.exchange_code === code
                ? { ...row, fee_bps: feeNum, is_broker_supported: s.is_supported }
                : row,
            ),
          );
        }
      } catch (e) {
        failed.push(`${code}: ${e instanceof Error ? e.message : String(e)}`);
        setState((prev) => ({
          ...prev,
          [code]: { ...prev[code], error: 'network error' },
        }));
      }
    }
    setSaving(false);
    if (failed.length > 0) {
      setSaveError(`Saved ${okCount} of ${pendingCount}. Failed: ${failed.slice(0, 3).join('; ')}${failed.length > 3 ? ` (+${failed.length - 3} more)` : ''}`);
    } else {
      setSaveOk(`Saved ${okCount} change${okCount === 1 ? '' : 's'}.`);
      setTimeout(() => setSaveOk(null), 3000);
    }
  };

  const handleReset = () => {
    setState((prev) => {
      const next: Record<string, RowState> = {};
      for (const [code, s] of Object.entries(prev)) {
        next[code] = {
          ...s,
          fee_bps: String(s.server_fee_bps),
          is_supported: s.server_is_supported,
          error: null,
        };
      }
      return next;
    });
    setSaveError(null);
    setSaveOk(null);
  };

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    let list = rows;
    if (showOnlyUnsupported) {
      list = list.filter((r) => state[r.exchange_code] && !state[r.exchange_code].is_supported);
    }
    if (q) {
      list = list.filter(
        (r) =>
          r.exchange_code.toLowerCase().includes(q) ||
          (r.exchange_name ?? '').toLowerCase().includes(q) ||
          (r.country_code ?? '').toLowerCase().includes(q) ||
          (r.currency_code ?? '').toLowerCase().includes(q),
      );
    }
    return list;
  }, [rows, search, showOnlyUnsupported, state]);

  const totals = useMemo(() => {
    let supported = 0, unsupported = 0, withFee = 0;
    for (const r of rows) {
      const s = state[r.exchange_code];
      if (!s) continue;
      if (s.is_supported) supported++;
      else unsupported++;
      const feeNum = s.fee_bps.trim() === '' ? 0 : Number(s.fee_bps);
      if (Number.isFinite(feeNum) && feeNum > 0) withFee++;
    }
    return { supported, unsupported, withFee };
  }, [rows, state]);

  return (
    <div className="min-h-screen bg-[#0f1117] text-gray-200">
      <div className="px-8 py-5 border-b border-gray-800/40">
        <h1 className="text-xl font-semibold text-white">Fees</h1>
        <p className="text-sm text-gray-500 mt-1">
          Per-exchange one-way transaction fees (basis points, 1 bp = 0.01%) and broker-support flags. Unsupported exchanges are dropped from the backtest universe entirely — every company on that exchange is excluded before signals are computed.
        </p>
      </div>

      <div className="px-8 py-6 max-w-6xl">
        {loadError && (
          <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-sm text-rose-300 mb-4">
            {loadError}
          </div>
        )}

        <div className="bg-[#151821] rounded-xl border border-gray-800/40">
          {/* Header: stats + filters + save controls */}
          <div className="px-5 py-3 border-b border-gray-800/40 flex items-center justify-between gap-3 flex-wrap">
            <div className="text-sm text-gray-400 flex items-center gap-4 flex-wrap">
              {loading ? 'Loading exchanges…' : (
                <>
                  <span><span className="text-gray-200 font-mono">{rows.length}</span> exchanges</span>
                  <span className="text-emerald-400">{totals.supported} supported</span>
                  <span className="text-rose-400">{totals.unsupported} unsupported</span>
                  <span className="text-amber-300">{totals.withFee} with fee</span>
                </>
              )}
            </div>
            <div className="flex items-center gap-2 flex-wrap">
              <label className="text-xs text-gray-400 inline-flex items-center gap-1.5 cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={showOnlyUnsupported}
                  onChange={(e) => setShowOnlyUnsupported(e.target.checked)}
                  className="accent-rose-500 w-3.5 h-3.5"
                />
                Show only unsupported
              </label>
              <input
                type="search"
                placeholder="Filter…"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-1.5 text-xs text-gray-200 placeholder-gray-500 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 focus:outline-none w-56"
              />
            </div>
          </div>

          <div className="overflow-auto">
            <table className="w-full text-sm">
              <thead className="text-gray-500 text-xs sticky top-0 bg-[#151821] z-10">
                <tr className="border-b border-gray-800/40">
                  <th className="text-center px-3 py-2.5 font-medium w-12">Supported</th>
                  <th className="text-left px-3 py-2.5 font-medium">Code</th>
                  <th className="text-left px-3 py-2.5 font-medium">Name</th>
                  <th className="text-left px-3 py-2.5 font-medium">Country</th>
                  <th className="text-left px-3 py-2.5 font-medium">Currency</th>
                  <th className="text-right px-3 py-2.5 font-medium">Fee (bps)</th>
                  <th className="text-right px-5 py-2.5 font-medium">≈ %</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((row) => {
                  const s = state[row.exchange_code];
                  if (!s) return null;
                  const pending = isRowPending(s);
                  const rowErr = validateRow(s);
                  const muted = !s.is_supported;
                  return (
                    <tr
                      key={row.exchange_code}
                      className={`border-b border-gray-800/20 hover:bg-white/[0.02] ${pending ? 'bg-indigo-500/[0.04]' : ''}`}
                    >
                      <td className="px-3 py-2 text-center">
                        <input
                          type="checkbox"
                          checked={s.is_supported}
                          onChange={(e) =>
                            setState((st) => ({
                              ...st,
                              [row.exchange_code]: { ...st[row.exchange_code], is_supported: e.target.checked, error: null },
                            }))
                          }
                          className="accent-emerald-500 w-4 h-4 cursor-pointer"
                          title={s.is_supported ? 'Supported by broker — companies on this exchange stay in the universe' : 'NOT supported — companies on this exchange are dropped before backtest'}
                        />
                      </td>
                      <td className={`px-3 py-2 font-mono ${muted ? 'text-gray-500' : 'text-gray-200'}`}>{row.exchange_code}</td>
                      <td className={`px-3 py-2 truncate max-w-[260px] ${muted ? 'text-gray-600' : 'text-gray-300'}`}>{row.exchange_name ?? '—'}</td>
                      <td className={`px-3 py-2 font-mono ${muted ? 'text-gray-600' : 'text-gray-500'}`}>{row.country_code ?? '—'}</td>
                      <td className={`px-3 py-2 font-mono ${muted ? 'text-gray-600' : 'text-gray-500'}`}>{row.currency_code ?? '—'}</td>
                      <td className="px-3 py-2 text-right">
                        <input
                          type="number"
                          min={0}
                          max={10000}
                          step="0.1"
                          value={s.fee_bps}
                          onChange={(e) =>
                            setState((st) => ({
                              ...st,
                              [row.exchange_code]: { ...st[row.exchange_code], fee_bps: e.target.value, error: null },
                            }))
                          }
                          className={`bg-[#0f1117] border rounded-lg px-2 py-1 text-xs font-mono w-24 text-right focus:ring-1 focus:outline-none ${
                            rowErr
                              ? 'border-rose-500/40 text-rose-300 focus:border-rose-400 focus:ring-rose-500/30'
                              : 'border-gray-700 text-gray-200 focus:border-indigo-500 focus:ring-indigo-500/30'
                          }`}
                        />
                        {rowErr && <div className="text-[10px] text-rose-400 mt-0.5">{rowErr}</div>}
                        {s.error && !rowErr && <div className="text-[10px] text-rose-400 mt-0.5">{s.error}</div>}
                      </td>
                      <td className={`px-5 py-2 text-right font-mono ${muted ? 'text-gray-600' : 'text-gray-500'}`}>{formatPct(s.server_fee_bps)}</td>
                    </tr>
                  );
                })}
                {!loading && filtered.length === 0 && (
                  <tr>
                    <td colSpan={7} className="px-5 py-6 text-sm text-gray-500 text-center">
                      {rows.length === 0 ? 'No exchanges configured.' : 'No exchanges match the filter.'}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* Sticky save bar — only renders when there's something to save
            so the page is quiet at rest. */}
        {pendingCount > 0 && (
          <div className="fixed bottom-6 left-1/2 -translate-x-1/2 bg-[#151821] border border-indigo-500/40 rounded-xl shadow-2xl px-5 py-3 flex items-center gap-4 z-30">
            <span className="text-sm text-gray-300">
              <span className="font-mono text-indigo-300">{pendingCount}</span> unsaved change{pendingCount === 1 ? '' : 's'}
            </span>
            <button
              type="button"
              onClick={handleReset}
              disabled={saving}
              className="text-xs text-gray-500 hover:text-gray-300 transition-colors disabled:opacity-50"
            >
              Reset
            </button>
            <button
              type="button"
              onClick={handleSave}
              disabled={saving}
              className="text-xs px-4 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white transition-colors"
            >
              {saving ? 'Saving…' : `Save ${pendingCount} change${pendingCount === 1 ? '' : 's'}`}
            </button>
          </div>
        )}

        {saveError && (
          <div className="mt-4 bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-sm text-rose-300">
            {saveError}
          </div>
        )}
        {saveOk && !saveError && (
          <div className="mt-4 bg-emerald-500/10 border border-emerald-500/20 rounded-lg px-4 py-3 text-sm text-emerald-300">
            {saveOk}
          </div>
        )}

        <p className="text-xs text-gray-500 mt-4">
          Uncheck an exchange to drop every company listed there from the backtest universe before signals are computed. Fees apply only to supported exchanges (no point computing fees on names you can&apos;t trade).
        </p>
      </div>
    </div>
  );
}
