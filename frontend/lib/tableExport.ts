/**
 * Shared table-export utilities. Two formats:
 *   - CSV: instant, zero dependencies, RFC 4180-style escaping.
 *   - XLSX: SheetJS-powered, lazy-loaded so the ~400KB module only
 *     hits the user's wire on first XLSX click.
 *
 * Both take the same shape: a list of rows and a list of `Column`
 * descriptors. The caller's `accessor` extracts the raw value per
 * row — exporters handle escaping, number formatting, and null
 * rendering so every table looks consistent in spreadsheets.
 *
 * Filenames are accepted WITHOUT extension; each exporter appends
 * its own + a date stamp so consecutive downloads in the same
 * session don't overwrite.
 */

export type Column<T> = {
  /** Stable internal key (also used as XLSX column key). */
  key: string;
  /** Header text rendered in the first row of the export. */
  header: string;
  /** Pulls the value for one row. Return `null`/`undefined` for blank cells.
   * Numbers stay numeric in XLSX (Excel will treat them as numbers, not text).
   * Strings get RFC-style escape in CSV. */
  accessor: (row: T) => string | number | boolean | null | undefined;
};

/** Today's date as `YYYY-MM-DD` for filename suffixes. */
function todayStamp(): string {
  const d = new Date();
  const y = d.getFullYear();
  const m = `${d.getMonth() + 1}`.padStart(2, '0');
  const day = `${d.getDate()}`.padStart(2, '0');
  return `${y}-${m}-${day}`;
}

/** Sanitize a basename — replace anything not safe for filenames with `_`. */
function sanitizeBasename(name: string): string {
  return name.replace(/[^a-zA-Z0-9._-]+/g, '_').replace(/^_+|_+$/g, '') || 'export';
}

/** RFC 4180-style CSV cell escaping: wrap in quotes if the value contains
 * `,` `"` `\n` `\r`, and double any internal quotes. Null/undefined → empty. */
function csvCell(v: unknown): string {
  if (v == null) return '';
  const s = typeof v === 'string' ? v : String(v);
  if (/[",\n\r]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

/** Trigger a browser download of a blob with the given filename. */
function triggerDownload(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  // Give the browser a beat to start the download before revoking; some
  // browsers cancel the download if the URL is revoked too quickly.
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/** Export rows to CSV and trigger download.
 * Filename pattern: `{basename}_{YYYY-MM-DD}.csv`. */
export function exportToCsv<T>(rows: T[], columns: Column<T>[], basename: string): void {
  const headerLine = columns.map((c) => csvCell(c.header)).join(',');
  const dataLines = rows.map((row) =>
    columns.map((c) => csvCell(c.accessor(row))).join(','),
  );
  // BOM up front so Excel detects UTF-8 instead of mis-decoding accented
  // names (Société, Aurubis, etc.) as Windows-1252.
  const csv = '﻿' + [headerLine, ...dataLines].join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
  const name = `${sanitizeBasename(basename)}_${todayStamp()}.csv`;
  triggerDownload(blob, name);
}

/** Export rows to XLSX and trigger download. Lazy-imports `xlsx` so the
 * bundle stays out of the initial page load.
 * Filename pattern: `{basename}_{YYYY-MM-DD}.xlsx`. */
export async function exportToXlsx<T>(
  rows: T[],
  columns: Column<T>[],
  basename: string,
): Promise<void> {
  const XLSX = await import('xlsx');
  // Build an array-of-arrays so column order is preserved (XLSX.json_to_sheet
  // honors object key insertion order, but explicit is safer).
  const header = columns.map((c) => c.header);
  const data = rows.map((row) =>
    columns.map((c) => {
      const v = c.accessor(row);
      if (v == null) return '';
      return v;
    }),
  );
  const aoa = [header, ...data];
  const ws = XLSX.utils.aoa_to_sheet(aoa);
  // Sensible default column widths — let the longer headers + numeric
  // columns breathe. Excel will still re-fit on AutoFit if the user
  // double-clicks the column divider.
  ws['!cols'] = columns.map((c) => ({
    wch: Math.max(8, Math.min(40, c.header.length + 4)),
  }));
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Data');
  const out = XLSX.write(wb, { type: 'array', bookType: 'xlsx' });
  const blob = new Blob([out], {
    type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  });
  const name = `${sanitizeBasename(basename)}_${todayStamp()}.xlsx`;
  triggerDownload(blob, name);
}
