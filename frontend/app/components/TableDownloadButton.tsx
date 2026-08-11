'use client';

import { useRef, useState } from 'react';
import { Column, exportToCsv, exportToXlsx } from '../../lib/tableExport';
import { useClickOutside, useEscapeKey } from '../../lib/hooks/useClickOutside';

type Props<T> = {
  /** Rows to export — pass the same already-filtered/sorted array the
   * table is rendering so the export matches what the user sees. */
  rows: T[];
  /** Column descriptors. Order here defines the export column order. */
  columns: Column<T>[];
  /** Filename without extension (e.g. "companies"). The exporter
   * appends a date stamp + the extension. */
  filename: string;
  /** Optional tooltip override. Defaults to "Download as CSV / XLSX". */
  title?: string;
  /** Extra classes on the trigger button (positioning, sizing). */
  className?: string;
  /** When set (e.g. "companies"), a confirmation dialog appears before the
   * download showing the row count + format ("Download 142 companies as
   * CSV?"). Omit to download immediately (the default). */
  confirmNoun?: string;
};

/** Small download icon + popover menu (CSV / XLSX). Drop this in any
 * table's header area, beside search inputs / count badges. */
export default function TableDownloadButton<T>({
  rows, columns, filename, title, className, confirmNoun,
}: Props<T>) {
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  // Column selection (only used in the `confirmNoun` picker). Defaults to ALL
  // columns; the user unticks any they don't want. Persists across opens.
  const [excluded, setExcluded] = useState<Set<string>>(new Set());
  const containerRef = useRef<HTMLDivElement>(null);

  useClickOutside(containerRef, () => setOpen(false), open);
  useEscapeKey(() => setOpen(false), open);

  const disabled = rows.length === 0 || busy;
  // Honour the user's column picks; the simple (no-picker) menu exports all.
  const exportCols = confirmNoun ? columns.filter((c) => !excluded.has(c.key)) : columns;

  const toggleCol = (key: string) =>
    setExcluded((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  const allSelected = excluded.size === 0;
  const toggleAll = () =>
    setExcluded((prev) => (prev.size === 0 ? new Set(columns.map((c) => c.key)) : new Set()));

  const handleCsv = () => {
    setOpen(false);
    if (exportCols.length === 0) return;
    exportToCsv(rows, exportCols, filename);
  };

  const handleXlsx = async () => {
    setOpen(false);
    if (exportCols.length === 0) return;
    setBusy(true);
    try {
      await exportToXlsx(rows, exportCols, filename);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div
      ref={containerRef}
      className={`relative inline-block ${className ?? ''}`}
      // Tables sometimes live inside CollapsibleCard buttons — stop the
      // download click from bubbling up and toggling the card.
      onClick={(e) => e.stopPropagation()}
      onKeyDown={(e) => e.stopPropagation()}
    >
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        disabled={disabled}
        title={title ?? (rows.length === 0 ? 'Nothing to download' : 'Download as CSV / XLSX')}
        aria-label="Download table"
        aria-haspopup="menu"
        aria-expanded={open}
        className="inline-flex items-center justify-center w-7 h-7 rounded-md text-fg-muted hover:text-accent-300 hover:bg-overlay/[0.04] transition-colors disabled:opacity-40 disabled:cursor-not-allowed disabled:hover:bg-transparent disabled:hover:text-fg-muted"
      >
        {/* Download icon — simple SVG so we don't pull in another lib. */}
        <svg viewBox="0 0 16 16" fill="currentColor" className="w-3.5 h-3.5" aria-hidden="true">
          <path d="M8 1a.75.75 0 0 1 .75.75v6.69l2.22-2.22a.75.75 0 1 1 1.06 1.06l-3.5 3.5a.75.75 0 0 1-1.06 0l-3.5-3.5a.75.75 0 1 1 1.06-1.06l2.22 2.22V1.75A.75.75 0 0 1 8 1Zm-5.25 11a.75.75 0 0 1 .75.75v.75c0 .14.11.25.25.25h8.5a.25.25 0 0 0 .25-.25v-.75a.75.75 0 0 1 1.5 0v.75A1.75 1.75 0 0 1 12.25 15.5h-8.5A1.75 1.75 0 0 1 2 13.75v-.75a.75.75 0 0 1 .75-.75Z" />
        </svg>
      </button>
      {open && !confirmNoun && (
        <div
          role="menu"
          className="absolute right-0 top-full mt-1 z-30 bg-popover border border-neutral-700 rounded-lg shadow-2xl min-w-[140px] py-1"
        >
          <button
            type="button"
            role="menuitem"
            onClick={handleCsv}
            className="w-full text-left px-3 py-1.5 text-xs text-fg hover:bg-overlay/[0.04] hover:text-fg-strong transition-colors flex items-center justify-between"
          >
            <span>Download CSV</span>
            <span className="text-fg-subtle text-[11px] font-mono">.csv</span>
          </button>
          <button
            type="button"
            role="menuitem"
            onClick={handleXlsx}
            disabled={busy}
            className="w-full text-left px-3 py-1.5 text-xs text-fg hover:bg-overlay/[0.04] hover:text-fg-strong transition-colors flex items-center justify-between disabled:opacity-50"
          >
            <span>{busy ? 'Building…' : 'Download XLSX'}</span>
            <span className="text-fg-subtle text-[11px] font-mono">.xlsx</span>
          </button>
        </div>
      )}

      {/* Rich picker: count + per-column checkboxes + format buttons. */}
      {open && confirmNoun && (
        <div className="absolute right-0 top-full mt-1 z-30 bg-popover border border-neutral-700 rounded-lg shadow-2xl w-64">
          <div className="px-3 py-2 border-b border-neutral-800/60 flex items-center justify-between">
            <span className="text-xs text-fg-muted">
              Export <span className="text-fg-strong font-medium">{rows.length}</span> {confirmNoun}
            </span>
            <button
              type="button"
              onClick={toggleAll}
              className="text-[12px] font-medium text-accent-400 hover:text-accent-500 transition-colors"
            >
              {allSelected ? 'Clear all' : 'Select all'}
            </button>
          </div>
          <div className="text-[11px] uppercase tracking-wider text-fg-subtle px-3 pt-1.5">
            Columns ({exportCols.length}/{columns.length})
          </div>
          <div className="max-h-56 overflow-auto px-1 py-1">
            {columns.map((c) => (
              <label
                key={c.key}
                className="flex items-center gap-2 px-2 py-1 text-xs cursor-pointer hover:bg-overlay/[0.04] rounded"
              >
                <input
                  type="checkbox"
                  checked={!excluded.has(c.key)}
                  onChange={() => toggleCol(c.key)}
                  className="accent-accent-500 w-3.5 h-3.5 shrink-0"
                />
                <span className="text-fg truncate">{c.header}</span>
              </label>
            ))}
          </div>
          <div className="flex gap-2 px-3 py-2 border-t border-neutral-800/60">
            <button
              type="button"
              onClick={handleCsv}
              disabled={exportCols.length === 0}
              className="flex-1 px-2 py-1.5 rounded-md text-xs font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              Download CSV
            </button>
            <button
              type="button"
              onClick={handleXlsx}
              disabled={exportCols.length === 0 || busy}
              className="flex-1 px-2 py-1.5 rounded-md text-xs font-medium border border-accent-500 text-accent-400 hover:bg-accent-600/10 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              {busy ? 'Building…' : 'XLSX'}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
