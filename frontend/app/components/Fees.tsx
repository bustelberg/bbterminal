'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import type { Column } from '../../lib/tableExport';
import TableDownloadButton from './TableDownloadButton';
import LoadingDots from './LoadingDots';
import { API_URL } from '../../lib/apiUrl';
import { DEFAULT_FEE_CONFIG, type FeeConfig } from './momentum/feeModel';

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

/** Per-row editable state for the broker-support toggle table. The
 * transaction COST is now a single global setting (Fee structure card
 * above), so the only editable per-exchange field is "supported". The
 * server's existing fee_bps is preserved verbatim on save. */
type RowState = {
  is_supported: boolean;
  server_is_supported: boolean;
  server_fee_bps: number;
  error: string | null;
};

function isRowPending(s: RowState): boolean {
  return s.is_supported !== s.server_is_supported;
}

// ─── Fee structure card (the four global parameters) ────────────────

type FeeField = { key: keyof FeeConfig; label: string; suffix: string; hint: string };

const LEONTEQ_FIELDS: FeeField[] = [
  { key: 'leonteq_annual_bps', label: 'Annual fee', suffix: 'bps / yr', hint: 'Deducted at each year-end (final year pro-rated).' },
  { key: 'transaction_bps', label: 'Transaction cost', suffix: 'bps / trade', hint: 'Charged on every buy and every sell, all exchanges.' },
];
const BUSTELBERG_FIELDS: FeeField[] = [
  { key: 'bustelberg_mgmt_bps', label: 'Management fee', suffix: 'bps / yr', hint: 'Deducted at each year-end (final year pro-rated).' },
  { key: 'bustelberg_perf_pct', label: 'Performance fee', suffix: '% HWM', hint: 'Charged yearly on gains above the running high-water mark.' },
];

function FeeStructureCard() {
  const [cfg, setCfg] = useState<Record<keyof FeeConfig, string>>(() => ({
    leonteq_annual_bps: String(DEFAULT_FEE_CONFIG.leonteq_annual_bps),
    transaction_bps: String(DEFAULT_FEE_CONFIG.transaction_bps),
    bustelberg_mgmt_bps: String(DEFAULT_FEE_CONFIG.bustelberg_mgmt_bps),
    bustelberg_perf_pct: String(DEFAULT_FEE_CONFIG.bustelberg_perf_pct),
  }));
  const [server, setServer] = useState<FeeConfig | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState<{ kind: 'ok' | 'err'; text: string } | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const r = await fetch(`${API_URL}/api/fee-config`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      const next: FeeConfig = {
        leonteq_annual_bps: Number(d.leonteq_annual_bps ?? DEFAULT_FEE_CONFIG.leonteq_annual_bps),
        transaction_bps: Number(d.transaction_bps ?? DEFAULT_FEE_CONFIG.transaction_bps),
        bustelberg_mgmt_bps: Number(d.bustelberg_mgmt_bps ?? DEFAULT_FEE_CONFIG.bustelberg_mgmt_bps),
        bustelberg_perf_pct: Number(d.bustelberg_perf_pct ?? DEFAULT_FEE_CONFIG.bustelberg_perf_pct),
      };
      setServer(next);
      setCfg({
        leonteq_annual_bps: String(next.leonteq_annual_bps),
        transaction_bps: String(next.transaction_bps),
        bustelberg_mgmt_bps: String(next.bustelberg_mgmt_bps),
        bustelberg_perf_pct: String(next.bustelberg_perf_pct),
      });
    } catch (e) {
      setMsg({ kind: 'err', text: e instanceof Error ? e.message : String(e) });
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  const parsed = useMemo<FeeConfig | null>(() => {
    const out = {} as FeeConfig;
    for (const k of Object.keys(cfg) as (keyof FeeConfig)[]) {
      const n = Number(cfg[k]);
      if (!Number.isFinite(n) || n < 0) return null;
      out[k] = n;
    }
    if (out.bustelberg_perf_pct > 100) return null;
    return out;
  }, [cfg]);

  const dirty = useMemo(() => {
    if (!server || !parsed) return false;
    return (Object.keys(parsed) as (keyof FeeConfig)[]).some((k) => parsed[k] !== server[k]);
  }, [server, parsed]);

  const save = async () => {
    if (!parsed) { setMsg({ kind: 'err', text: 'All values must be ≥ 0 (performance fee ≤ 100%).' }); return; }
    setSaving(true);
    setMsg(null);
    try {
      const r = await apiFetch(`${API_URL}/api/fee-config`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(parsed),
      });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setMsg({ kind: 'err', text: `Save failed: ${r.status} ${body.slice(0, 120)}` });
        return;
      }
      setServer(parsed);
      setMsg({ kind: 'ok', text: 'Fee structure saved.' });
      setTimeout(() => setMsg(null), 3000);
    } catch (e) {
      setMsg({ kind: 'err', text: e instanceof Error ? e.message : String(e) });
    } finally {
      setSaving(false);
    }
  };

  const field = (f: FeeField) => (
    <div key={f.key} className="flex items-center justify-between gap-3 py-2">
      <div className="min-w-0">
        <div className="text-sm text-fg">{f.label}</div>
        <div className="text-[11px] text-fg-faint">{f.hint}</div>
      </div>
      <div className="flex items-center gap-2 shrink-0">
        <input
          type="number"
          min={0}
          step="1"
          value={cfg[f.key]}
          onChange={(e) => setCfg((c) => ({ ...c, [f.key]: e.target.value }))}
          className="bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-sm font-mono w-24 text-right text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
        />
        <span className="text-xs text-fg-subtle w-20">{f.suffix}</span>
      </div>
    </div>
  );

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40 mb-6">
      <div className="px-5 py-3 border-b border-neutral-800/40 flex items-center justify-between">
        <h2 className="text-sm font-medium text-fg-strong">Fee structure</h2>
        {loading && <LoadingDots label="Loading" />}
      </div>
      <div className="px-5 py-4 grid gap-x-8 gap-y-2 md:grid-cols-2">
        <div>
          <div className="text-[10px] uppercase tracking-wider text-fg-subtle mb-1">Leonteq costs</div>
          <div className="divide-y divide-neutral-800/30">{LEONTEQ_FIELDS.map(field)}</div>
        </div>
        <div>
          <div className="text-[10px] uppercase tracking-wider text-fg-subtle mb-1">Bustelberg fees</div>
          <div className="divide-y divide-neutral-800/30">{BUSTELBERG_FIELDS.map(field)}</div>
        </div>
      </div>
      <div className="px-5 py-3 border-t border-neutral-800/40 flex items-center justify-between gap-3">
        <span className="text-xs text-fg-faint">
          Applied to every backtest&apos;s fee waterfall: gross → after Leonteq → after Bustelberg.
        </span>
        <div className="flex items-center gap-3">
          {msg && (
            <span className={`text-xs ${msg.kind === 'ok' ? 'text-pos-300' : 'text-neg-300'}`}>
              {msg.text}
            </span>
          )}
          <button
            type="button"
            onClick={() => void save()}
            disabled={saving || !dirty || !parsed}
            className="text-xs px-4 py-1.5 rounded-lg bg-accent-600 hover:bg-accent-500 disabled:opacity-40 disabled:cursor-not-allowed text-fg-strong transition-colors"
          >
            {saving ? 'Saving…' : dirty ? 'Save fee structure' : 'Saved'}
          </button>
        </div>
      </div>
    </div>
  );
}

// ─── Broker-supported exchange table ────────────────────────────────

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
          is_supported: row.is_broker_supported,
          server_is_supported: row.is_broker_supported,
          server_fee_bps: row.fee_bps,
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
    setSaving(true);
    setSaveError(null);
    setSaveOk(null);
    let okCount = 0;
    const failed: string[] = [];
    // Sequential PUTs — at most ~50 exchanges. Each preserves the row's
    // existing fee_bps (the transaction cost is global now) and only
    // flips is_broker_supported.
    for (const code of pendingCodes) {
      const s = state[code];
      try {
        const r = await apiFetch(`${API_URL}/api/exchange-fees/${encodeURIComponent(code)}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            fee_bps: s.server_fee_bps,
            is_broker_supported: s.is_supported,
          }),
        });
        if (!r.ok) {
          const body = await r.text().catch(() => '');
          failed.push(`${code}: ${r.status} ${body.slice(0, 80)}`);
          setState((prev) => ({ ...prev, [code]: { ...prev[code], error: `${r.status}` } }));
        } else {
          okCount++;
          setState((prev) => ({
            ...prev,
            [code]: { ...prev[code], server_is_supported: s.is_supported, error: null },
          }));
          setRows((rs) =>
            rs.map((row) =>
              row.exchange_code === code ? { ...row, is_broker_supported: s.is_supported } : row,
            ),
          );
        }
      } catch (e) {
        failed.push(`${code}: ${e instanceof Error ? e.message : String(e)}`);
        setState((prev) => ({ ...prev, [code]: { ...prev[code], error: 'network error' } }));
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
        next[code] = { ...s, is_supported: s.server_is_supported, error: null };
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
    let supported = 0, unsupported = 0;
    for (const r of rows) {
      const s = state[r.exchange_code];
      if (!s) continue;
      if (s.is_supported) supported++;
      else unsupported++;
    }
    return { supported, unsupported };
  }, [rows, state]);

  const exportColumns = useMemo<Column<ExchangeFee>[]>(() => [
    { key: 'exchange_code', header: 'Code', accessor: (r) => r.exchange_code },
    { key: 'exchange_name', header: 'Name', accessor: (r) => r.exchange_name ?? '' },
    { key: 'country_code', header: 'Country', accessor: (r) => r.country_code ?? '' },
    { key: 'currency_code', header: 'Currency', accessor: (r) => r.currency_code ?? '' },
    { key: 'is_supported', header: 'Supported', accessor: (r) => (r.is_broker_supported ? 'yes' : 'no') },
    { key: 'is_us', header: 'US', accessor: (r) => (r.is_us ? 'yes' : 'no') },
  ], []);

  return (
    <div className="min-h-screen bg-page text-fg">
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">Fees</h1>
        <p className="text-sm text-fg-subtle mt-1">
          The global fee structure (Leonteq + Bustelberg) feeds every backtest&apos;s fee waterfall. Below, mark which exchanges your broker can trade — unsupported exchanges are dropped from the backtest universe entirely before signals are computed.
        </p>
      </div>

      <div className="px-8 py-6 max-w-6xl">
        <FeeStructureCard />

        {loadError && (
          <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3 text-sm text-neg-300 mb-4">
            {loadError}
          </div>
        )}

        <div className="bg-card rounded-xl border border-neutral-800/40">
          <div className="px-5 py-3 border-b border-neutral-800/40 flex items-center justify-between gap-3 flex-wrap">
            <div className="text-sm text-fg-muted flex items-center gap-4 flex-wrap">
              {loading ? <LoadingDots label="Loading exchanges" /> : (
                <>
                  <span className="text-fg-strong font-medium">Broker-supported exchanges</span>
                  <span><span className="text-fg font-mono">{rows.length}</span> exchanges</span>
                  <span className="text-pos-400">{totals.supported} supported</span>
                  <span className="text-neg-400">{totals.unsupported} unsupported</span>
                </>
              )}
            </div>
            <div className="flex items-center gap-2 flex-wrap">
              <label className="text-xs text-fg-muted inline-flex items-center gap-1.5 cursor-pointer">
                <input
                  type="checkbox"
                  checked={showOnlyUnsupported}
                  onChange={(e) => setShowOnlyUnsupported(e.target.checked)}
                  className="accent-neg-500 w-3.5 h-3.5"
                />
                Show only unsupported
              </label>
              <input
                type="search"
                placeholder="Filter…"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-xs text-fg placeholder-fg-subtle focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 focus:outline-none w-56"
              />
              <TableDownloadButton
                rows={filtered}
                columns={exportColumns}
                filename="exchange_support"
                title={`Download ${filtered.length} exchanges as CSV / XLSX`}
              />
            </div>
          </div>

          <div className="overflow-auto">
            <table className="w-full text-sm">
              <thead className="text-fg-subtle text-xs sticky top-0 bg-card z-10">
                <tr className="border-b border-neutral-800/40">
                  <th className="text-center px-3 py-2.5 font-medium w-12">Supported</th>
                  <th className="text-left px-3 py-2.5 font-medium">Code</th>
                  <th className="text-left px-3 py-2.5 font-medium">Name</th>
                  <th className="text-left px-3 py-2.5 font-medium">Country</th>
                  <th className="text-left px-3 py-2.5 font-medium">Currency</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((row) => {
                  const s = state[row.exchange_code];
                  if (!s) return null;
                  const pending = isRowPending(s);
                  const muted = !s.is_supported;
                  return (
                    <tr
                      key={row.exchange_code}
                      className={`border-b border-neutral-800/20 hover:bg-overlay/[0.02] ${pending ? 'bg-accent-500/[0.04]' : ''}`}
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
                          className="accent-pos-500 w-4 h-4 cursor-pointer"
                          title={s.is_supported ? 'Supported by broker — companies on this exchange stay in the universe' : 'NOT supported — companies on this exchange are dropped before backtest'}
                        />
                        {s.error && <div className="text-[10px] text-neg-400 mt-0.5">{s.error}</div>}
                      </td>
                      <td className={`px-3 py-2 font-mono ${muted ? 'text-fg-subtle' : 'text-fg'}`}>{row.exchange_code}</td>
                      <td className={`px-3 py-2 truncate max-w-[260px] ${muted ? 'text-fg-faint' : 'text-fg-soft'}`}>{row.exchange_name ?? '—'}</td>
                      <td className={`px-3 py-2 font-mono ${muted ? 'text-fg-faint' : 'text-fg-subtle'}`}>{row.country_code ?? '—'}</td>
                      <td className={`px-3 py-2 font-mono ${muted ? 'text-fg-faint' : 'text-fg-subtle'}`}>{row.currency_code ?? '—'}</td>
                    </tr>
                  );
                })}
                {!loading && filtered.length === 0 && (
                  <tr>
                    <td colSpan={5} className="px-5 py-6 text-sm text-fg-subtle text-center">
                      {rows.length === 0 ? 'No exchanges configured.' : 'No exchanges match the filter.'}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {pendingCount > 0 && (
          <div className="fixed bottom-6 left-1/2 -translate-x-1/2 bg-card border border-accent-500/40 rounded-xl shadow-2xl px-5 py-3 flex items-center gap-4 z-30">
            <span className="text-sm text-fg-soft">
              <span className="font-mono text-accent-300">{pendingCount}</span> unsaved change{pendingCount === 1 ? '' : 's'}
            </span>
            <button
              type="button"
              onClick={handleReset}
              disabled={saving}
              className="text-xs text-fg-subtle hover:text-fg-soft transition-colors disabled:opacity-50"
            >
              Reset
            </button>
            <button
              type="button"
              onClick={handleSave}
              disabled={saving}
              className="text-xs px-4 py-1.5 rounded-lg bg-accent-600 hover:bg-accent-500 disabled:opacity-50 text-fg-strong transition-colors"
            >
              {saving ? 'Saving…' : `Save ${pendingCount} change${pendingCount === 1 ? '' : 's'}`}
            </button>
          </div>
        )}

        {saveError && (
          <div className="mt-4 bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3 text-sm text-neg-300">
            {saveError}
          </div>
        )}
        {saveOk && !saveError && (
          <div className="mt-4 bg-pos-500/10 border border-pos-500/20 rounded-lg px-4 py-3 text-sm text-pos-300">
            {saveOk}
          </div>
        )}

        <p className="text-xs text-fg-subtle mt-4">
          Uncheck an exchange to drop every company listed there from the backtest universe before signals are computed.
        </p>
      </div>
    </div>
  );
}
