'use client';

import { useEffect, useRef, useState } from 'react';
import { Column, exportToCsv, exportToXlsx } from '../../lib/tableExport';

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
};

/** Small download icon + popover menu (CSV / XLSX). Drop this in any
 * table's header area, beside search inputs / count badges. */
export default function TableDownloadButton<T>({
  rows, columns, filename, title, className,
}: Props<T>) {
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  // Close the menu on outside click + Escape.
  useEffect(() => {
    const onDoc = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false);
    };
    document.addEventListener('mousedown', onDoc);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onDoc);
      document.removeEventListener('keydown', onKey);
    };
  }, []);

  const disabled = rows.length === 0 || busy;

  const handleCsv = () => {
    setOpen(false);
    exportToCsv(rows, columns, filename);
  };

  const handleXlsx = async () => {
    setOpen(false);
    setBusy(true);
    try {
      await exportToXlsx(rows, columns, filename);
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
        className="inline-flex items-center justify-center w-7 h-7 rounded-md text-gray-400 hover:text-indigo-300 hover:bg-white/[0.04] transition-colors disabled:opacity-40 disabled:cursor-not-allowed disabled:hover:bg-transparent disabled:hover:text-gray-400"
      >
        {/* Download icon — simple SVG so we don't pull in another lib. */}
        <svg viewBox="0 0 16 16" fill="currentColor" className="w-3.5 h-3.5" aria-hidden="true">
          <path d="M8 1a.75.75 0 0 1 .75.75v6.69l2.22-2.22a.75.75 0 1 1 1.06 1.06l-3.5 3.5a.75.75 0 0 1-1.06 0l-3.5-3.5a.75.75 0 1 1 1.06-1.06l2.22 2.22V1.75A.75.75 0 0 1 8 1Zm-5.25 11a.75.75 0 0 1 .75.75v.75c0 .14.11.25.25.25h8.5a.25.25 0 0 0 .25-.25v-.75a.75.75 0 0 1 1.5 0v.75A1.75 1.75 0 0 1 12.25 15.5h-8.5A1.75 1.75 0 0 1 2 13.75v-.75a.75.75 0 0 1 .75-.75Z" />
        </svg>
      </button>
      {open && (
        <div
          role="menu"
          className="absolute right-0 top-full mt-1 z-30 bg-[#1e2130] border border-gray-700 rounded-lg shadow-2xl min-w-[140px] py-1"
        >
          <button
            type="button"
            role="menuitem"
            onClick={handleCsv}
            className="w-full text-left px-3 py-1.5 text-xs text-gray-200 hover:bg-white/[0.04] hover:text-white transition-colors flex items-center justify-between"
          >
            <span>Download CSV</span>
            <span className="text-gray-500 text-[10px] font-mono">.csv</span>
          </button>
          <button
            type="button"
            role="menuitem"
            onClick={handleXlsx}
            disabled={busy}
            className="w-full text-left px-3 py-1.5 text-xs text-gray-200 hover:bg-white/[0.04] hover:text-white transition-colors flex items-center justify-between disabled:opacity-50"
          >
            <span>{busy ? 'Building…' : 'Download XLSX'}</span>
            <span className="text-gray-500 text-[10px] font-mono">.xlsx</span>
          </button>
        </div>
      )}
    </div>
  );
}
