'use client';

import { useState } from 'react';
import { dialog } from '../../../lib/dialog';
import LoadingDots from '../LoadingDots';
import type { Etf } from './useDiversifier';

/** One selectable benchmark list — used twice on the diversifier page: once
 * for Bonds (the core sleeve) and once for diversifier ETFs. Selection +
 * data come from `useDiversifier`; this is presentational. Each row can be
 * moved to the OTHER section via `onMove` (the bond/ETF categorization). */
export default function BenchmarkSection({
  title, hint, rows, selectedIds, onToggle, onSelectAll, accent,
  onAdd, adding, addPlaceholder, onRefresh, onDelete, busyId, onMove, moveLabel,
  emptyText, loading,
}: {
  title: string;
  hint: string;
  rows: Etf[];
  selectedIds: Set<number>;
  onToggle: (id: number) => void;
  onSelectAll: () => void;
  accent: 'accent' | 'warn';
  onAdd: (ticker: string) => void;
  adding: boolean;
  addPlaceholder: string;
  onRefresh: (id: number) => void;
  onDelete: (id: number) => void;
  busyId: number | null;
  /** Optional — when set, each row shows a button to move it to the other
   * section (the bond/ETF split). Omit for a single unified list. */
  onMove?: (id: number) => void;
  moveLabel?: string;
  emptyText: string;
  loading: boolean;
}) {
  const [ticker, setTicker] = useState('');
  const selectedCount = rows.filter((e) => selectedIds.has(e.benchmark_id)).length;
  const allSelected = rows.length > 0 && rows.every((e) => selectedIds.has(e.benchmark_id));
  const accentCls = accent === 'warn' ? 'accent-warn-500' : 'accent-accent-500';

  const submit = () => { onAdd(ticker); setTicker(''); };

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
      <div className="flex items-center justify-between mb-1 gap-3 flex-wrap">
        <h3 className="text-fg-muted text-xs font-medium uppercase tracking-wider">
          {title}
          {selectedCount > 0 && <span className={accent === 'warn' ? 'text-warn-400' : 'text-accent-400'}> · {selectedCount} selected</span>}
        </h3>
        {rows.length > 0 && (
          <button onClick={onSelectAll} className="text-xs font-medium text-accent-400 hover:text-accent-500 transition-colors">
            {allSelected ? 'Clear all' : 'Select all'}
          </button>
        )}
      </div>
      <p className="text-xs text-fg-subtle mb-3">{hint}</p>

      <div className="flex items-end gap-3 mb-4">
        <input
          value={ticker}
          onChange={(e) => setTicker(e.target.value)}
          placeholder={addPlaceholder}
          className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono w-40 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none placeholder-fg-faint"
          onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
        />
        <button
          onClick={submit}
          disabled={adding || !ticker.trim()}
          className="px-4 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-40"
        >
          {adding ? <LoadingDots /> : 'Add & fetch'}
        </button>
      </div>

      {rows.length === 0 ? (
        <p className="text-sm text-fg-subtle">{loading ? 'Loading…' : emptyText}</p>
      ) : (
        <div className="overflow-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-fg-subtle text-xs border-b border-neutral-800/60">
                <th className="text-center font-medium py-2 px-2 w-8"></th>
                <th className="text-left font-medium py-2 px-2">Ticker</th>
                <th className="text-left font-medium py-2 px-2">Name</th>
                <th className="text-left font-medium py-2 px-2">Prices from</th>
                <th className="text-left font-medium py-2 px-2">Prices to</th>
                <th className="text-right font-medium py-2 px-2"></th>
              </tr>
            </thead>
            <tbody>
              {rows.map((e) => (
                <tr key={e.benchmark_id} className="group border-b border-neutral-800/40 hover:bg-overlay/[0.02]">
                  <td className="py-2.5 px-2 text-center">
                    <input
                      type="checkbox"
                      checked={selectedIds.has(e.benchmark_id)}
                      onChange={() => onToggle(e.benchmark_id)}
                      className={`${accentCls} w-4 h-4 cursor-pointer`}
                    />
                  </td>
                  <td className="py-2.5 px-2 font-mono text-fg-strong">{e.ticker}</td>
                  <td className="py-2.5 px-2 text-fg">{e.name}</td>
                  <td className="py-2.5 px-2 font-mono text-fg-muted">{e.price_from ?? '—'}</td>
                  <td className="py-2.5 px-2 font-mono text-fg-muted">{e.price_to ?? '—'}</td>
                  <td className="py-2.5 px-2 text-right whitespace-nowrap">
                    {onMove && (
                      <button
                        onClick={() => onMove(e.benchmark_id)}
                        className="opacity-0 group-hover:opacity-100 text-xs text-fg-muted hover:text-accent-400 transition-all mr-3"
                        title="Move to the other section"
                      >
                        {moveLabel}
                      </button>
                    )}
                    <button
                      onClick={() => onRefresh(e.benchmark_id)}
                      disabled={busyId === e.benchmark_id}
                      className="opacity-0 group-hover:opacity-100 text-xs text-fg-muted hover:text-accent-400 transition-all disabled:opacity-50 mr-3"
                    >
                      {busyId === e.benchmark_id ? '…' : 'Refresh'}
                    </button>
                    <button
                      onClick={async () => {
                        if (await dialog.confirm(`Delete "${e.ticker}"?`, { destructive: true, confirmLabel: 'Delete' })) {
                          onDelete(e.benchmark_id);
                        }
                      }}
                      disabled={busyId === e.benchmark_id}
                      className="opacity-0 group-hover:opacity-100 text-xs text-fg-muted hover:text-neg-400 transition-all disabled:opacity-50"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
