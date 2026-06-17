'use client';

import { useState, useEffect, useCallback, useMemo } from 'react';
import { dialog } from '../../lib/dialog';
import { apiFetch } from '../../lib/apiFetch';
import { trackedFetch } from '../../lib/loading';
import { API_URL } from '../../lib/apiUrl';
import type { Column } from '../../lib/tableExport';
import TableDownloadButton from './TableDownloadButton';
import LoadingDots from './LoadingDots';

/**
 * Uniform "frozen universes" panel shared by every universe page (/sp500,
 * /acwi, /leonteq, /longequity-universe). It lists the frozen static snapshots
 * taken from a page's source(s), lets you INSPECT a snapshot's constituents,
 * DELETE one, and (optionally) freeze a new one — so the experience is the
 * same everywhere.
 *
 * Everything resolves by universe *label*, which works for any universe:
 *   - list   → GET /api/static-universes        (filtered by `frozen_from`)
 *   - inspect→ GET /api/index-universe/months + /tickers?index=<label>
 *   - delete → DELETE /api/universe/labels/<label>
 *   - freeze → the page's own mechanic, passed in via `onFreeze`
 */

type FrozenSnapshot = {
  universe_id: number;
  label: string;
  frozen_from: string | null;
  frozen_at: string | null;
  latest_captured_month: string | null;
  latest_membership_count: number;
};
type MonthEntry = { target_month: string; count: number };
type TickerEntry = {
  ticker: string;
  company_id: number | null;
  company_name: string | null;
  isin: string | null;
  exchange: string | null;
  gurufocus_url: string;
};

export default function FrozenUniversesPanel({
  frozenFrom,
  title = 'Frozen snapshots',
  description,
  onFreeze,
  freezeLabel = 'Freeze snapshot',
  freezeDisabled = false,
  freezeHint,
  reloadSignal = 0,
}: {
  /** Only list snapshots whose `frozen_from` is one of these source keys
   * (e.g. ['SP500'], ['ACWI'], ['LongEquity'], ['LEONTEQ']). */
  frozenFrom: string[];
  title?: string;
  description?: string;
  /** Page-specific freeze action. When provided, a uniform "Freeze" button is
   * rendered; on success the list refreshes. Resolve with a result message or
   * void (the panel shows a generic confirmation). */
  onFreeze?: () => Promise<{ ok: boolean; text: string } | void>;
  freezeLabel?: string;
  freezeDisabled?: boolean;
  freezeHint?: string;
  /** Bump from the parent to force a list refetch (after an external freeze,
   * e.g. an SSE flow that has its own button). */
  reloadSignal?: number;
}) {
  const [snapshots, setSnapshots] = useState<FrozenSnapshot[]>([]);
  const [loadingList, setLoadingList] = useState(false);
  const [listError, setListError] = useState<string | null>(null);

  const [inspected, setInspected] = useState<string | null>(null);
  const [inspectedMonth, setInspectedMonth] = useState<string | null>(null);
  const [tickers, setTickers] = useState<TickerEntry[]>([]);
  const [loadingTickers, setLoadingTickers] = useState(false);
  const [filter, setFilter] = useState('');

  const [freezing, setFreezing] = useState(false);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);

  const fromKey = frozenFrom.join(',');

  const loadList = useCallback(() => {
    setLoadingList(true);
    setListError(null);
    const allow = new Set(fromKey.split(',').filter(Boolean));
    trackedFetch('Loading frozen universes', `${API_URL}/api/static-universes`)
      .then(async r => {
        if (!r.ok) throw new Error(`Couldn't load snapshots (${r.status})`);
        return r.json();
      })
      .then((data: FrozenSnapshot[]) => {
        // A non-array body (e.g. an error object) is a failure, not "empty" —
        // surface it instead of silently showing "No frozen snapshots yet".
        if (!Array.isArray(data)) throw new Error("Couldn't load snapshots (unexpected response)");
        setSnapshots(data.filter(s => s.frozen_from && allow.has(s.frozen_from)));
      })
      .catch((e: unknown) => {
        setListError(e instanceof Error ? e.message : "Couldn't load snapshots");
      })
      .finally(() => setLoadingList(false));
  }, [fromKey]);

  useEffect(() => { loadList(); }, [loadList, reloadSignal]);

  // Inspect a snapshot's constituents — resolve by label (works for any
  // universe). Picks its single captured month.
  const inspect = useCallback((label: string) => {
    setInspected(label);
    setFilter('');
    setLoadingTickers(true);
    setTickers([]);
    setInspectedMonth(null);
    trackedFetch(`Loading ${label} months`, `${API_URL}/api/index-universe/months?index=${encodeURIComponent(label)}`)
      .then(r => r.json())
      .then((months: MonthEntry[]) => {
        const month = months.length > 0 ? months[months.length - 1].target_month : null;
        setInspectedMonth(month);
        if (!month) { setLoadingTickers(false); return; }
        return trackedFetch(
          `Loading ${label} constituents`,
          `${API_URL}/api/index-universe/tickers?index=${encodeURIComponent(label)}&month=${month}`,
        )
          .then(r => r.json())
          .then((t: TickerEntry[]) => { setTickers(Array.isArray(t) ? t : []); setLoadingTickers(false); });
      })
      .catch(() => setLoadingTickers(false));
  }, []);

  const remove = useCallback(async (label: string) => {
    if (!(await dialog.confirm(`Delete the frozen universe "${label}"?`, { destructive: true, confirmLabel: 'Delete' }))) return;
    try {
      const r = await apiFetch(`${API_URL}/api/universe/labels/${encodeURIComponent(label)}`, { method: 'DELETE' });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        setMsg({ ok: false, text: d.detail ?? `Delete failed (${r.status})` });
        return;
      }
      if (inspected === label) { setInspected(null); setTickers([]); setInspectedMonth(null); }
      setMsg({ ok: true, text: `Deleted "${label}".` });
      loadList();
    } catch (e) {
      setMsg({ ok: false, text: e instanceof Error ? e.message : String(e) });
    }
  }, [inspected, loadList]);

  const doFreeze = useCallback(async () => {
    if (!onFreeze || freezing) return;
    setFreezing(true);
    setMsg(null);
    try {
      const res = await onFreeze();
      if (res) setMsg(res);
      else setMsg({ ok: true, text: 'Frozen.' });
      loadList();
    } catch (e) {
      setMsg({ ok: false, text: e instanceof Error ? e.message : String(e) });
    } finally {
      setFreezing(false);
    }
  }, [onFreeze, freezing, loadList]);

  const filteredTickers = useMemo(() => (
    filter
      ? tickers.filter(t =>
          t.ticker.toLowerCase().includes(filter.toLowerCase()) ||
          (t.company_name || '').toLowerCase().includes(filter.toLowerCase()) ||
          (t.isin || '').toLowerCase().includes(filter.toLowerCase()) ||
          (t.exchange || '').toLowerCase().includes(filter.toLowerCase()))
      : tickers
  ), [tickers, filter]);

  const exportColumns = useMemo<Column<TickerEntry>[]>(() => [
    { key: 'ticker', header: 'Ticker', accessor: (t) => t.ticker },
    { key: 'exchange', header: 'Exchange', accessor: (t) => t.exchange ?? '' },
    { key: 'isin', header: 'ISIN', accessor: (t) => t.isin ?? '' },
    { key: 'company_id', header: 'Company ID', accessor: (t) => t.company_id ?? '' },
    { key: 'company_name', header: 'Company', accessor: (t) => t.company_name ?? '' },
    { key: 'gurufocus_url', header: 'GuruFocus URL', accessor: (t) => t.gurufocus_url },
  ], []);

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40">
      <div className="px-5 py-4 border-b border-neutral-800/40 flex items-start justify-between gap-3 flex-wrap">
        <div className="min-w-0">
          <h3 className="text-sm font-medium text-fg-strong">{title}</h3>
          <p className="text-fg-subtle text-xs mt-0.5">
            {description ?? 'Static, pipeline-immune copies — click one to inspect its companies. These are the reproducible universes selectable in /backtest.'}
          </p>
        </div>
        {onFreeze && (
          <button
            onClick={() => void doFreeze()}
            disabled={freezing || freezeDisabled}
            title={freezeHint ?? freezeLabel}
            className="shrink-0 px-3 py-1.5 rounded-lg text-xs font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {freezing ? 'Freezing…' : freezeLabel}
          </button>
        )}
      </div>

      {msg && (
        <div className={`px-5 py-2 text-xs flex items-start justify-between gap-3 border-b border-neutral-800/40 ${msg.ok ? 'text-pos-300' : 'text-neg-300'}`}>
          <span>{msg.text}</span>
          <button type="button" onClick={() => setMsg(null)} className="text-fg-subtle hover:text-fg-soft shrink-0">dismiss</button>
        </div>
      )}

      <div className="p-5">
        {loadingList && snapshots.length === 0 ? (
          <div className="text-sm text-fg-subtle"><LoadingDots label="Loading" /></div>
        ) : listError ? (
          <div className="text-sm text-neg-300 flex items-center gap-3">
            <span>{listError}</span>
            <button
              type="button"
              onClick={() => loadList()}
              className="px-2 py-1 rounded-md text-xs bg-overlay/5 hover:bg-overlay/10 text-fg-soft transition-colors"
            >
              Retry
            </button>
          </div>
        ) : snapshots.length === 0 ? (
          <p className="text-sm text-fg-subtle">
            No frozen snapshots yet.{onFreeze ? ' Use the button above to create one.' : ''}
          </p>
        ) : (
          <div className="grid gap-3">
            {snapshots.map(s => (
              <div
                key={s.universe_id}
                onClick={() => inspect(s.label)}
                className={`flex items-center justify-between px-4 py-3 rounded-lg cursor-pointer transition-colors border ${
                  inspected === s.label
                    ? 'bg-accent-600/10 border-accent-500/30'
                    : 'bg-page border-neutral-800/40 hover:bg-overlay/[0.02]'
                }`}
              >
                <div className="min-w-0">
                  <span className="text-fg-strong font-medium text-sm truncate">{s.label}</span>
                  {s.frozen_at && (
                    <span className="text-fg-subtle text-xs ml-3">
                      frozen {new Date(s.frozen_at).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })}
                    </span>
                  )}
                </div>
                <div className="flex items-center gap-4 shrink-0">
                  <span className="text-xs text-fg-muted">{s.latest_membership_count} companies</span>
                  <button
                    onClick={e => { e.stopPropagation(); void remove(s.label); }}
                    className="text-fg-faint hover:text-neg-400 text-xs transition-colors"
                  >
                    Delete
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Constituents of the inspected snapshot */}
      {inspected && (
        <div className="border-t border-neutral-800/40">
          <div className="px-5 py-4 border-b border-neutral-800/40 flex items-center justify-between gap-3 flex-wrap">
            <h4 className="text-sm font-medium text-fg-strong">
              {inspected}{inspectedMonth ? ` — as of ${inspectedMonth}` : ''} — {tickers.length} companies
              {tickers.length > 0 && (
                <span className="text-fg-subtle font-normal ml-2">
                  ({tickers.filter(t => t.company_id).length} matched)
                </span>
              )}
            </h4>
            <div className="flex items-center gap-2">
              <input
                type="text"
                placeholder="Filter..."
                value={filter}
                onChange={e => setFilter(e.target.value)}
                className="px-3 py-1.5 bg-page border border-neutral-700 rounded-lg text-sm text-fg placeholder-fg-faint focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none w-48"
              />
              <TableDownloadButton
                rows={filteredTickers}
                columns={exportColumns}
                filename={`frozen_${inspected}_${inspectedMonth ?? 'constituents'}`}
                title={`Download ${filteredTickers.length} companies as CSV / XLSX`}
              />
              <button
                onClick={() => { setInspected(null); setTickers([]); setInspectedMonth(null); }}
                className="text-fg-faint hover:text-fg-soft text-xs transition-colors"
              >
                Close
              </button>
            </div>
          </div>
          <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
            {loadingTickers ? (
              <div className="px-5 py-8 text-center text-fg-subtle text-sm"><LoadingDots label="Loading" /></div>
            ) : (
              <table className="w-full text-sm">
                <thead className="sticky top-0 bg-card">
                  <tr className="text-left text-xs text-fg-subtle border-b border-neutral-800/40">
                    <th className="px-5 py-2.5 font-medium w-12">#</th>
                    <th className="px-3 py-2.5 font-medium">Ticker</th>
                    <th className="px-3 py-2.5 font-medium">Exchange</th>
                    <th className="px-3 py-2.5 font-medium">ISIN</th>
                    <th className="px-3 py-2.5 font-medium">Company</th>
                    <th className="px-3 py-2.5 font-medium">GuruFocus</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredTickers.map((t, i) => (
                    <tr key={`${t.ticker}-${t.company_id ?? i}`} className="border-b border-neutral-800/20 hover:bg-overlay/[0.02]">
                      <td className="px-5 py-2.5 text-fg-faint font-mono">{i + 1}</td>
                      <td className="px-3 py-2.5 text-fg-strong font-mono font-medium">{t.ticker || '—'}</td>
                      <td className="px-3 py-2.5 text-fg-muted font-mono text-xs">{t.exchange || '—'}</td>
                      <td className="px-3 py-2.5 text-fg-muted font-mono text-xs">{t.isin || '—'}</td>
                      <td className="px-3 py-2.5 text-fg-soft">{t.company_name || '—'}</td>
                      <td className="px-3 py-2.5">
                        <a href={t.gurufocus_url} target="_blank" rel="noopener noreferrer" className="text-xs text-accent-400 hover:text-accent-300 transition-colors">
                          View
                        </a>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
