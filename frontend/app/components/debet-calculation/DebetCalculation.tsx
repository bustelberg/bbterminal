'use client';

import { ChangeEvent, DragEvent, Fragment, useRef, useState } from 'react';
import * as XLSX from 'xlsx';
import { downloadCalculationXlsx } from './calculationExport';
import { type DebetCopy, useDebetCopy } from './debetCopy';

type ClientTotal = {
  portfolio: string;
  name: string;
  cash: number;
  tradeTotal: number;
};

type SortKey = 'portfolio' | 'name' | 'cash' | 'tradeTotal' | 'projectedCash';
type SortDirection = 'asc' | 'desc';

type ParsedFile = {
  name: string;
  sheetName: string;
  rows: { portfolio: string; name: string; cash: number; trade: number; portfolioCell: string; hidden: boolean }[];
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

function eur(value: number, locale: string): string {
  return new Intl.NumberFormat(locale, { style: 'currency', currency: 'EUR' }).format(value);
}

function portfolioKey(portfolio: string): string {
  // This is an account identifier, not a display label. Do not fold casing or
  // otherwise fuzz-match it: a source line is valid only for the same code.
  return portfolio.trim();
}

async function parseFile(file: File, t: DebetCopy): Promise<ParsedFile> {
  // SheetJS drops row metadata by default. `cellStyles` is what retains the
  // `!rows[index].hidden` state that Excel uses for hidden/filter-hidden rows,
  // allowing the checked "only unhidden rows" option below to do real work.
  const workbook = XLSX.read(await file.arrayBuffer(), { type: 'array', cellDates: false, cellStyles: true });
  if (workbook.SheetNames.length !== 1) {
    throw new Error(t.wrongTabCount(file.name, workbook.SheetNames.length));
  }
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const rawRows = XLSX.utils.sheet_to_json<unknown[]>(sheet, { header: 1, defval: null, raw: true });
  const rowMetadata = sheet['!rows'] ?? [];
  // SheetJS uses zero-based row metadata; Excel displays one-based row numbers.
  const headerIndex = rawRows.findIndex((row) => Array.isArray(row) && row.some((cell) => String(cell ?? '').trim()));
  if (headerIndex < 0) throw new Error(t.emptyFile(file.name));

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
    throw new Error(t.missingColumns(file.name, missing));
  }

  const rows: ParsedFile['rows'] = [];
  rawRows.slice(headerIndex + 1).forEach((row, offset) => {
    const portfolio = String(row[portfolioColumn] ?? '').trim();
    if (!portfolio) return; // Blank trailing rows are normal in exported workbooks.
    const name = String(row[nameColumn] ?? '').trim();
    const cash = numberFromCell(row[cashColumn]);
    const trade = numberFromCell(row[tradeColumn]);
    if (cash === null || trade === null) {
      throw new Error(t.invalidValues(file.name, headerIndex + offset + 2));
    }
    const excelRow = headerIndex + offset + 2; // Excel's row numbers are one-based.
    rows.push({
      portfolio,
      name,
      cash,
      trade,
      portfolioCell: `${XLSX.utils.encode_col(portfolioColumn)}${excelRow}`,
      hidden: Boolean(rowMetadata[headerIndex + offset + 1]?.hidden),
    });
  });
  if (rows.length === 0) throw new Error(t.noClientRows(file.name));
  return { name: file.name, sheetName: workbook.SheetNames[0], rows };
}

export default function DebetCalculation() {
  const t = useDebetCopy();
  const input = useRef<HTMLInputElement>(null);
  const [files, setFiles] = useState<ParsedFile[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [dragging, setDragging] = useState(false);
  const [expandedClients, setExpandedClients] = useState<Set<string>>(() => new Set());
  const [useHiddenRows, setUseHiddenRows] = useState(false);
  const [search, setSearch] = useState('');
  const [sortKey, setSortKey] = useState<SortKey>('portfolio');
  const [sortDirection, setSortDirection] = useState<SortDirection>('asc');

  // Keep the imported source intact. Toggling the filter must re-run the
  // calculation immediately, without making the user upload every file again.
  const calculationFiles = useHiddenRows
    ? files
    : files.map((file) => ({ ...file, rows: file.rows.filter((row) => !row.hidden) }));

  const totals = Array.from(calculationFiles.reduce((clients, file) => {
    // First collapse THIS workbook only. A workbook can contribute to a client
    // only when it has an explicit row with that exact Portefeuille value.
    const fileClients = new Map<string, { portfolio: string; name: string; cash: number; trade: number }>();
    for (const row of file.rows) {
      const key = portfolioKey(row.portfolio);
      const current = fileClients.get(key);
      if (current) {
        current.trade += row.trade;
      } else {
        fileClients.set(key, { portfolio: row.portfolio, name: row.name, cash: row.cash, trade: row.trade });
      }
    }

    for (const [key, fileClient] of fileClients) {
      const current = clients.get(key);
      if (current) {
        current.tradeTotal += fileClient.trade;
      } else {
        clients.set(key, {
          portfolio: fileClient.portfolio,
          name: fileClient.name,
          cash: fileClient.cash,
          tradeTotal: fileClient.trade,
        });
      }
    }
    return clients;
  }, new Map<string, ClientTotal>()).values()).sort((a, b) => a.portfolio.localeCompare(b.portfolio, 'nl'));
  const searchTerm = search.trim().toLocaleLowerCase('nl-NL');
  const filteredTotals = searchTerm
    ? totals.filter((client) => [client.portfolio, client.name].some((value) => value.toLocaleLowerCase('nl-NL').includes(searchTerm)))
    : totals;
  const sortedTotals = [...filteredTotals].sort((a, b) => {
    const value = (client: ClientTotal): string | number => {
      switch (sortKey) {
        case 'portfolio': return client.portfolio;
        case 'name': return client.name;
        case 'cash': return client.cash;
        case 'tradeTotal': return client.tradeTotal;
        case 'projectedCash': return client.cash - client.tradeTotal;
      }
    };
    const left = value(a);
    const right = value(b);
    const compared = typeof left === 'string' && typeof right === 'string'
      ? left.localeCompare(right, 'nl-NL')
      : Number(left) - Number(right);
    return (compared || a.portfolio.localeCompare(b.portfolio, 'nl-NL')) * (sortDirection === 'asc' ? 1 : -1);
  });

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDirection((current) => current === 'asc' ? 'desc' : 'asc');
    else { setSortKey(key); setSortDirection('asc'); }
  }


  async function addFiles(incoming: FileList | File[]) {
    const selected = Array.from(incoming);
    if (!selected.length) return;
    const invalid = selected.find((file) => !/\.(xlsx|xlsm|xls)$/i.test(file.name));
    if (invalid) {
      setError(t.invalidFile(invalid.name));
      return;
    }
    try {
      const parsed = await Promise.all(selected.map((file) => parseFile(file, t)));
      setFiles((current) => [...current, ...parsed]);
      setError(null);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : t.unreadableFile);
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
        <p className="text-xs font-semibold uppercase tracking-[0.16em] text-accent-400">{t.cashControl}</p>
        <h1 className="mt-1 text-2xl font-semibold tracking-tight text-fg-strong">{t.title}</h1>
        <p className="mt-2 max-w-3xl text-sm text-fg-muted">{t.intro}</p>
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
        <p className="text-sm font-medium text-fg-strong">{t.dropFiles}</p>
        <p className="mt-1 text-xs text-fg-subtle">{t.acceptedFiles}</p>
      </div>

      {error && <div role="alert" className="mt-4 rounded-lg border border-neg-500/40 bg-neg-500/10 px-4 py-3 text-sm text-neg-300">{error}</div>}

      {files.length > 0 && (
        <section className="mt-5 rounded-xl border border-neutral-800/70 bg-panel p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-sm font-semibold text-fg-strong">{t.uploadedFiles(files.length)}</h2>
              <p className="mt-0.5 text-xs text-fg-subtle">{t.localProcessing}</p>
            </div>
            <button type="button" onClick={() => { setFiles([]); setError(null); }} className="rounded-md border border-neutral-700 px-3 py-1.5 text-xs font-medium text-fg-muted transition-colors hover:border-neutral-500 hover:text-fg-strong">{t.clearAll}</button>
          </div>
          <div className="mt-3 flex flex-wrap gap-2">
            {files.map((file, index) => (
              <div key={`${file.name}-${index}`} className="flex items-center gap-2 rounded-md bg-overlay/5 py-1.5 pl-2.5 pr-1.5 text-xs text-fg-muted">
                <span>{file.name} <span className="text-fg-subtle">({file.rows.length})</span></span>
                <button type="button" aria-label={t.removeFile(file.name)} onClick={() => setFiles((current) => current.filter((_, i) => i !== index))} className="rounded px-1 text-fg-subtle hover:bg-overlay/10 hover:text-neg-300">×</button>
              </div>
            ))}
          </div>
          <label className="mt-4 flex w-fit cursor-pointer items-center gap-2 text-xs text-fg-muted">
            <input type="checkbox" checked={useHiddenRows} onChange={(event) => setUseHiddenRows(event.target.checked)} className="h-3.5 w-3.5 accent-accent-500" />
            {t.useHiddenRows}
          </label>
        </section>
      )}

      {totals.length > 0 && <>
        <section className="mt-5 overflow-hidden rounded-xl border border-neutral-800/70 bg-panel">
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-neutral-800/70 px-4 py-3">
            <h2 className="text-sm font-semibold text-fg-strong">{t.clientCalculation}</h2>
            <div className="flex flex-wrap items-center gap-2">
              <label className="sr-only" htmlFor="debet-client-search">{t.searchLabel}</label>
              <input id="debet-client-search" type="search" value={search} onChange={(event) => setSearch(event.target.value)} placeholder={t.searchPlaceholder} className="h-7 w-52 rounded-md border border-neutral-700 bg-page px-2 text-xs text-fg-strong placeholder:text-fg-subtle outline-none transition-colors focus:border-accent-400" />
              <button type="button" onClick={() => downloadCalculationXlsx(sortedTotals, calculationFiles)} className="rounded-md border border-neutral-700 px-2 py-1 text-xs font-medium text-fg-muted hover:border-neutral-500 hover:text-fg-strong">{t.downloadExcel}</button>
            </div>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full min-w-[760px] text-sm">
              <thead className="bg-overlay/[0.035] text-left text-xs font-medium text-fg-subtle">
                <tr>
                  <SortableHeader label={t.portfolio} column="portfolio" active={sortKey} direction={sortDirection} onSort={toggleSort} />
                  <SortableHeader label={t.name} column="name" active={sortKey} direction={sortDirection} onSort={toggleSort} />
                  <SortableHeader label={t.liquidCash} column="cash" active={sortKey} direction={sortDirection} onSort={toggleSort} align="right" />
                  <SortableHeader label={t.purchasesSales} column="tradeTotal" active={sortKey} direction={sortDirection} onSort={toggleSort} align="right" />
                  <SortableHeader label={t.projectedCash} column="projectedCash" active={sortKey} direction={sortDirection} onSort={toggleSort} align="right" />
                  <th className="px-4 py-3"><span className="sr-only">{t.calculationDetails}</span></th>
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-800/60">
                {sortedTotals.map((client) => {
                  const remaining = client.cash - client.tradeTotal;
                  const negative = remaining < -0.005;
                  const isExpanded = expandedClients.has(client.portfolio);
                  // Deliberately derive this from the original rows rather than
                  // retaining a second aggregated source map. That makes it
                  // impossible for a file to appear unless it has this exact
                  // Portefeuille value itself.
                  const sourceTrades = calculationFiles.flatMap((file) => {
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
                      <td className="px-4 py-3 text-right tabular-nums text-fg-muted">{eur(client.cash, t.locale)}</td>
                      <td className={`px-4 py-3 text-right tabular-nums ${client.tradeTotal > 0 ? 'text-warn-300' : 'text-good-300'}`}>{eur(client.tradeTotal, t.locale)}</td>
                      <td className={`px-4 py-3 text-right font-semibold tabular-nums ${negative ? 'text-neg-300' : 'text-good-300'}`}>{eur(remaining, t.locale)}</td>
                      <td className="px-4 py-3 text-right"><button type="button" aria-expanded={isExpanded} onClick={() => setExpandedClients((current) => { const next = new Set(current); if (next.has(client.portfolio)) next.delete(client.portfolio); else next.add(client.portfolio); return next; })} className="rounded-md border border-neutral-700 px-2 py-1 text-xs font-medium text-fg-muted hover:border-neutral-500 hover:text-fg-strong">{isExpanded ? t.hideCalculation : t.showCalculation}</button></td>
                    </tr>
                    {isExpanded && <tr className="bg-overlay/[0.035]"><td colSpan={6} className="px-4 py-4"><div className="max-w-2xl rounded-lg border border-neutral-800/70 bg-page px-4 py-3 text-xs"><p className="font-semibold text-fg-strong">{t.calculation}</p><div className="mt-2 space-y-1.5 tabular-nums"><div className="flex justify-between gap-8 text-fg-muted"><span>{t.startingLiquidCash}</span><span>{eur(client.cash, t.locale)}</span></div>{sourceTrades.map(({ source, trade, locations }) => <div key={source} className="flex justify-between gap-8 text-fg-muted"><span className="min-w-0"><span className="block truncate" title={source}>{trade >= 0 ? '−' : '+'} {t.buySellFrom(source)}</span><span className="block pt-0.5 font-mono text-[11px] text-fg-subtle">{t.foundAt(locations.join(', '))}</span></span><span className={trade >= 0 ? 'text-warn-300' : 'text-good-300'}>{trade >= 0 ? '−' : '+'}{eur(Math.abs(trade), t.locale)}</span></div>)}<div className={`mt-2 flex justify-between gap-8 border-t border-neutral-800/70 pt-2 font-semibold ${negative ? 'text-neg-300' : 'text-good-300'}`}><span>{t.projectedCash}</span><span>{eur(remaining, t.locale)}</span></div></div></div></td></tr>}
                  </Fragment>;
                })}
              </tbody>
            </table>
          </div>
        </section>
      </>}
    </main>
  );
}

function SortableHeader({ label, column, active, direction, onSort, align = 'left' }: {
  label: string;
  column: SortKey;
  active: SortKey;
  direction: SortDirection;
  onSort: (column: SortKey) => void;
  align?: 'left' | 'right';
}) {
  const isActive = active === column;
  return <th aria-sort={isActive ? (direction === 'asc' ? 'ascending' : 'descending') : 'none'} className={`px-4 py-3 ${align === 'right' ? 'text-right' : ''}`}>
    <button type="button" onClick={() => onSort(column)} className={`inline-flex items-center gap-1 hover:text-fg-strong ${align === 'right' ? 'justify-end' : ''}`}>
      {label}<span aria-hidden="true" className={isActive ? 'text-accent-300' : 'text-fg-faint'}>{isActive ? (direction === 'asc' ? '↑' : '↓') : '↕'}</span>
    </button>
  </th>;
}
