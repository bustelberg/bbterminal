'use client';

import { ChangeEvent, DragEvent, Fragment, useRef, useState } from 'react';
import * as XLSX from 'xlsx';

type ClientTotal = {
  portfolio: string;
  name: string;
  cash: number;
  tradeTotal: number;
  files: number;
  cashMismatch: boolean;
};

type ParsedFile = {
  name: string;
  sheetName: string;
  hiddenRows: number[];
  rows: { portfolio: string; name: string; cash: number; trade: number; portfolioCell: string }[];
};

const REQUIRED_COLUMNS = {
  portfolio: 'Portefeuille',
  name: 'Naam',
  cash: 'Totale waarde liquide middelen',
  trade: 'Aan te kopen waarde',
} as const;

function normaliseHeader(value: unknown): string {
  return String(value ?? '')
    .trim()
    .toLocaleLowerCase('nl-NL')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ');
}

function numberFromCell(value: unknown): number | null {
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (typeof value !== 'string') return null;

  let source = value.trim().replace(/\u00a0/g, '').replace(/€/g, '');
  if (!source || /^[-–—]$/.test(source)) return null;
  const negative = /^\(.*\)$/.test(source);
  source = source.replace(/[()\s]/g, '');

  // Dutch exports often use 1.234,56; imports can use 1,234.56.
  const comma = source.lastIndexOf(',');
  const dot = source.lastIndexOf('.');
  if (comma >= 0 && dot >= 0) {
    source = comma > dot ? source.replace(/\./g, '').replace(',', '.') : source.replace(/,/g, '');
  } else if (comma >= 0) {
    source = source.replace(',', '.');
  }
  const parsed = Number(source);
  return Number.isFinite(parsed) ? (negative ? -Math.abs(parsed) : parsed) : null;
}

function eur(value: number): string {
  return new Intl.NumberFormat('nl-NL', { style: 'currency', currency: 'EUR' }).format(value);
}

function portfolioKey(portfolio: string): string {
  // This is an account identifier, not a display label. Do not fold casing or
  // otherwise fuzz-match it: a source line is valid only for the same code.
  return portfolio.trim();
}

async function parseFile(file: File): Promise<ParsedFile> {
  const workbook = XLSX.read(await file.arrayBuffer(), { type: 'array', cellDates: false });
  if (workbook.SheetNames.length !== 1) {
    throw new Error(`${file.name} has ${workbook.SheetNames.length} tabs; exactly one tab is required.`);
  }
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const rawRows = XLSX.utils.sheet_to_json<unknown[]>(sheet, { header: 1, defval: null, raw: true });
  const rowMetadata = sheet['!rows'] ?? [];
  // SheetJS uses zero-based row metadata; Excel displays one-based row numbers.
  const hiddenRows = rowMetadata.flatMap((metadata, index) => metadata?.hidden ? [index + 1] : []);
  const headerIndex = rawRows.findIndex((row) => Array.isArray(row) && row.some((cell) => String(cell ?? '').trim()));
  if (headerIndex < 0) throw new Error(`${file.name} is empty.`);

  const header = rawRows[headerIndex].map(normaliseHeader);
  const column = (label: string) => header.indexOf(normaliseHeader(label));
  const portfolioColumn = column(REQUIRED_COLUMNS.portfolio);
  const nameColumn = column(REQUIRED_COLUMNS.name);
  const cashColumn = column(REQUIRED_COLUMNS.cash);
  const tradeColumn = column(REQUIRED_COLUMNS.trade);
  if ([portfolioColumn, nameColumn, cashColumn, tradeColumn].some((index) => index < 0)) {
    const missing = [
      portfolioColumn < 0 && REQUIRED_COLUMNS.portfolio,
      nameColumn < 0 && REQUIRED_COLUMNS.name,
      cashColumn < 0 && REQUIRED_COLUMNS.cash,
      tradeColumn < 0 && REQUIRED_COLUMNS.trade,
    ].filter(Boolean).join(', ');
    throw new Error(`${file.name} is missing column(s): ${missing}.`);
  }

  const rows: ParsedFile['rows'] = [];
  rawRows.slice(headerIndex + 1).forEach((row, offset) => {
    // SheetJS reads hidden rows too. For an order export, a client hidden by
    // an Excel filter must not silently affect the visible calculation.
    if (rowMetadata[headerIndex + offset + 1]?.hidden) return;
    const portfolio = String(row[portfolioColumn] ?? '').trim();
    if (!portfolio) return; // Blank trailing rows are normal in exported workbooks.
    const name = String(row[nameColumn] ?? '').trim();
    const cash = numberFromCell(row[cashColumn]);
    const trade = numberFromCell(row[tradeColumn]);
    if (cash === null || trade === null) {
      throw new Error(`${file.name}, row ${headerIndex + offset + 2}: cash and purchase value must both be numbers.`);
    }
    const excelRow = headerIndex + offset + 2; // Excel's row numbers are one-based.
    rows.push({ portfolio, name, cash, trade, portfolioCell: `${XLSX.utils.encode_col(portfolioColumn)}${excelRow}` });
  });
  if (rows.length === 0) throw new Error(`${file.name} has no client rows.`);
  return { name: file.name, sheetName: workbook.SheetNames[0], hiddenRows, rows };
}

export default function DebetCalculation() {
  const input = useRef<HTMLInputElement>(null);
  const [files, setFiles] = useState<ParsedFile[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [dragging, setDragging] = useState(false);
  const [expandedClients, setExpandedClients] = useState<Set<string>>(() => new Set());

  const totals = Array.from(files.reduce((clients, file) => {
    // First collapse THIS workbook only. A workbook can contribute to a client
    // only when it has an explicit row with that exact Portefeuille value.
    const fileClients = new Map<string, { portfolio: string; name: string; cash: number; trade: number; rows: number }>();
    for (const row of file.rows) {
      const key = portfolioKey(row.portfolio);
      const current = fileClients.get(key);
      if (current) {
        current.trade += row.trade;
        current.rows += 1;
      } else {
        fileClients.set(key, { portfolio: row.portfolio, name: row.name, cash: row.cash, trade: row.trade, rows: 1 });
      }
    }

    for (const [key, fileClient] of fileClients) {
      const current = clients.get(key);
      if (current) {
        current.tradeTotal += fileClient.trade;
        current.files += fileClient.rows;
        current.cashMismatch ||= Math.abs(current.cash - fileClient.cash) > 0.005;
      } else {
        clients.set(key, {
          portfolio: fileClient.portfolio,
          name: fileClient.name,
          cash: fileClient.cash,
          tradeTotal: fileClient.trade,
          files: fileClient.rows,
          cashMismatch: false,
        });
      }
    }
    return clients;
  }, new Map<string, ClientTotal>()).values()).sort((a, b) => a.portfolio.localeCompare(b.portfolio, 'nl'));

  const totalCash = totals.reduce((sum, client) => sum + client.cash, 0);
  const tradeTotal = totals.reduce((sum, client) => sum + client.tradeTotal, 0);
  const projectedCash = totals.reduce((sum, client) => sum + client.cash - client.tradeTotal, 0);
  const negativeClients = totals.filter((client) => client.cash - client.tradeTotal < -0.005);

  async function addFiles(incoming: FileList | File[]) {
    const selected = Array.from(incoming);
    if (!selected.length) return;
    const invalid = selected.find((file) => !/\.(xlsx|xlsm|xls)$/i.test(file.name));
    if (invalid) {
      setError(`${invalid.name} is not an Excel file. Upload .xlsx, .xlsm, or .xls files.`);
      return;
    }
    try {
      const parsed = await Promise.all(selected.map(parseFile));
      setFiles((current) => [...current, ...parsed]);
      setError(null);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'The Excel file could not be read.');
    } finally {
      if (input.current) input.current.value = '';
    }
  }

  function onDrop(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    setDragging(false);
    void addFiles(event.dataTransfer.files);
  }

  return (
    <main className="max-w-7xl mx-auto px-4 py-6 sm:px-6 lg:px-8 lg:py-8">
      <div className="mb-7">
        <p className="text-xs font-semibold uppercase tracking-[0.16em] text-accent-400">Cash control</p>
        <h1 className="mt-1 text-2xl font-semibold tracking-tight text-fg-strong">Debet calculations</h1>
        <p className="mt-2 max-w-3xl text-sm text-fg-muted">
          Upload one or more order files to combine each client&apos;s purchases and sales with their liquid cash position.
          A positive purchase value reduces cash; a negative value is a sale and increases it.
        </p>
      </div>

      <input ref={input} type="file" className="sr-only" multiple accept=".xlsx,.xlsm,.xls,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel" onChange={(event: ChangeEvent<HTMLInputElement>) => void addFiles(event.target.files ?? [])} />
      <div
        role="button"
        tabIndex={0}
        onClick={() => input.current?.click()}
        onKeyDown={(event) => { if (event.key === 'Enter' || event.key === ' ') input.current?.click(); }}
        onDragOver={(event) => { event.preventDefault(); setDragging(true); }}
        onDragLeave={() => setDragging(false)}
        onDrop={onDrop}
        className={`cursor-pointer rounded-xl border-2 border-dashed px-6 py-10 text-center transition-colors ${dragging ? 'border-accent-400 bg-accent-500/10' : 'border-neutral-700 bg-panel hover:border-accent-500/70 hover:bg-accent-500/[0.04]'}`}
      >
        <div className="mx-auto mb-3 flex h-11 w-11 items-center justify-center rounded-full bg-accent-500/15 text-xl text-accent-300">↑</div>
        <p className="text-sm font-medium text-fg-strong">Drop Excel files here, or click to upload</p>
        <p className="mt-1 text-xs text-fg-subtle">.xlsx, .xlsm, or .xls · one tab per file</p>
      </div>

      {error && <div role="alert" className="mt-4 rounded-lg border border-neg-500/40 bg-neg-500/10 px-4 py-3 text-sm text-neg-300">{error}</div>}

      {files.length > 0 && (
        <section className="mt-5 rounded-xl border border-neutral-800/70 bg-panel p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-sm font-semibold text-fg-strong">Uploaded files ({files.length})</h2>
              <p className="mt-0.5 text-xs text-fg-subtle">All processing happens locally in this browser.</p>
            </div>
            <button type="button" onClick={() => { setFiles([]); setError(null); }} className="rounded-md border border-neutral-700 px-3 py-1.5 text-xs font-medium text-fg-muted transition-colors hover:border-neutral-500 hover:text-fg-strong">Clear all</button>
          </div>
          <div className="mt-3 flex flex-wrap gap-2">
            {files.map((file, index) => (
              <div key={`${file.name}-${index}`} className="flex items-center gap-2 rounded-md bg-overlay/5 py-1.5 pl-2.5 pr-1.5 text-xs text-fg-muted">
                <span>{file.name} <span className="text-fg-subtle">({file.rows.length})</span></span>
                <button type="button" aria-label={`Remove ${file.name}`} onClick={() => setFiles((current) => current.filter((_, i) => i !== index))} className="rounded px-1 text-fg-subtle hover:bg-overlay/10 hover:text-neg-300">×</button>
              </div>
            ))}
          </div>
          {files.some((file) => (file.hiddenRows?.length ?? 0) > 0) && <div className="mt-4 rounded-lg border border-warn-500/30 bg-warn-500/[0.07] px-3 py-2.5 text-xs text-warn-200"><p className="font-semibold">Hidden Excel rows ignored</p>{files.filter((file) => (file.hiddenRows?.length ?? 0) > 0).map((file) => <p key={`${file.name}-hidden`} className="mt-1 text-warn-100/90"><span className="font-medium">{file.name} — {file.sheetName}:</span> rows {file.hiddenRows.join(', ')}</p>)}</div>}
        </section>
      )}

      {totals.length > 0 && <>
        <section className="mt-6 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <Stat label="Clients" value={String(totals.length)} />
          <Stat label="Starting liquid cash" value={eur(totalCash)} />
          <Stat label="Net purchases / sales" value={eur(tradeTotal)} tone={tradeTotal > 0 ? 'warn' : 'good'} />
          <Stat label="Projected liquid cash" value={eur(projectedCash)} tone={projectedCash < 0 ? 'bad' : 'good'} />
        </section>

        {negativeClients.length > 0 && <div className="mt-5 rounded-xl border border-neg-500/40 bg-neg-500/10 px-4 py-3 text-sm text-neg-200"><span className="font-semibold">{negativeClients.length} client{negativeClients.length === 1 ? '' : 's'} below zero.</span> Their projected cash position is highlighted in the table.</div>}

        <section className="mt-5 overflow-hidden rounded-xl border border-neutral-800/70 bg-panel">
          <div className="border-b border-neutral-800/70 px-4 py-3"><h2 className="text-sm font-semibold text-fg-strong">Client calculation</h2></div>
          <div className="overflow-x-auto">
            <table className="w-full min-w-[760px] text-sm">
              <thead className="bg-overlay/[0.035] text-left text-xs font-medium text-fg-subtle">
                <tr><th className="px-4 py-3">Portefeuille</th><th className="px-4 py-3">Naam</th><th className="px-4 py-3 text-right">Liquid cash</th><th className="px-4 py-3 text-right">Purchases / sales</th><th className="px-4 py-3 text-right">Projected cash</th><th className="px-4 py-3 text-right">Rows</th><th className="px-4 py-3">Status</th><th className="px-4 py-3"><span className="sr-only">Calculation details</span></th></tr>
              </thead>
              <tbody className="divide-y divide-neutral-800/60">
                {totals.map((client) => {
                  const remaining = client.cash - client.tradeTotal;
                  const negative = remaining < -0.005;
                  const isExpanded = expandedClients.has(client.portfolio);
                  // Deliberately derive this from the original rows rather than
                  // retaining a second aggregated source map. That makes it
                  // impossible for a file to appear unless it has this exact
                  // Portefeuille value itself.
                  const sourceTrades = files.flatMap((file) => {
                    const matchingRows = file.rows.filter((row) => portfolioKey(row.portfolio) === portfolioKey(client.portfolio));
                    if (matchingRows.length === 0) return [];
                    return [{
                      source: `${file.name} — ${file.sheetName}`,
                      trade: matchingRows.reduce((sum, row) => sum + row.trade, 0),
                      locations: matchingRows.map((row) => `${file.sheetName}!${row.portfolioCell}`),
                    }];
                  });
                  return <Fragment key={client.portfolio}>
                    <tr className={negative ? 'bg-neg-500/[0.07]' : ''}>
                      <td className="px-4 py-3 font-medium text-fg-strong">{client.portfolio}</td>
                      <td className="px-4 py-3 text-fg-muted">{client.name || '—'}</td>
                      <td className="px-4 py-3 text-right tabular-nums text-fg-muted">{eur(client.cash)}</td>
                      <td className={`px-4 py-3 text-right tabular-nums ${client.tradeTotal > 0 ? 'text-warn-300' : 'text-good-300'}`}>{eur(client.tradeTotal)}</td>
                      <td className={`px-4 py-3 text-right font-semibold tabular-nums ${negative ? 'text-neg-300' : 'text-good-300'}`}>{eur(remaining)}</td>
                      <td className="px-4 py-3 text-right tabular-nums text-fg-muted">{client.files}</td>
                      <td className="px-4 py-3">{negative ? <span className="rounded-full bg-neg-500/15 px-2 py-1 text-xs font-medium text-neg-300">Below zero</span> : client.cashMismatch ? <span className="rounded-full bg-warn-500/15 px-2 py-1 text-xs font-medium text-warn-300">Cash differs</span> : <span className="rounded-full bg-good-500/15 px-2 py-1 text-xs font-medium text-good-300">Covered</span>}</td>
                      <td className="px-4 py-3 text-right"><button type="button" aria-expanded={isExpanded} onClick={() => setExpandedClients((current) => { const next = new Set(current); if (next.has(client.portfolio)) next.delete(client.portfolio); else next.add(client.portfolio); return next; })} className="rounded-md border border-neutral-700 px-2 py-1 text-xs font-medium text-fg-muted hover:border-neutral-500 hover:text-fg-strong">{isExpanded ? 'Hide calculation' : 'Show calculation'}</button></td>
                    </tr>
                    {isExpanded && <tr className="bg-overlay/[0.035]"><td colSpan={8} className="px-4 py-4"><div className="max-w-2xl rounded-lg border border-neutral-800/70 bg-page px-4 py-3 text-xs"><p className="font-semibold text-fg-strong">Calculation</p><div className="mt-2 space-y-1.5 tabular-nums"><div className="flex justify-between gap-8 text-fg-muted"><span>Starting liquid cash</span><span>{eur(client.cash)}</span></div>{sourceTrades.map(({ source, trade, locations }) => <div key={source} className="flex justify-between gap-8 text-fg-muted"><span className="min-w-0"><span className="block truncate" title={source}>{trade >= 0 ? '−' : '+'} Buy / sell from {source}</span><span className="block pt-0.5 font-mono text-[11px] text-fg-subtle">Found at {locations.join(', ')}</span></span><span className={trade >= 0 ? 'text-warn-300' : 'text-good-300'}>{trade >= 0 ? '−' : '+'}{eur(Math.abs(trade))}</span></div>)}<div className={`mt-2 flex justify-between gap-8 border-t border-neutral-800/70 pt-2 font-semibold ${negative ? 'text-neg-300' : 'text-good-300'}`}><span>Projected liquid cash</span><span>{eur(remaining)}</span></div></div></div></td></tr>}
                  </Fragment>;
                })}
              </tbody>
            </table>
          </div>
          <p className="border-t border-neutral-800/70 px-4 py-3 text-xs text-fg-subtle">“Cash differs” means the client&apos;s liquid-cash value was not identical across uploaded rows; the first encountered value is used for the calculation.</p>
        </section>
      </>}
    </main>
  );
}

function Stat({ label, value, tone }: { label: string; value: string; tone?: 'good' | 'warn' | 'bad' }) {
  const color = tone === 'bad' ? 'text-neg-300' : tone === 'warn' ? 'text-warn-300' : tone === 'good' ? 'text-good-300' : 'text-fg-strong';
  return <div className="rounded-xl border border-neutral-800/70 bg-panel px-4 py-4"><p className="text-xs font-medium text-fg-subtle">{label}</p><p className={`mt-1 text-xl font-semibold tracking-tight tabular-nums ${color}`}>{value}</p></div>;
}
