'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import type { AssetGridRow } from '../../lib/types/api';
import AssetChartModal from './AssetChartModal';

/** Median daily traded value, EUR — compact. */
const adv = (v: number | null | undefined) =>
  v == null ? '—' : v >= 1e9 ? `€${(v / 1e9).toFixed(2)}B` : v >= 1e6 ? `€${(v / 1e6).toFixed(1)}M` : v >= 1e3 ? `€${(v / 1e3).toFixed(0)}k` : `€${v.toFixed(0)}`;

// Resolution status → dot colour: ok=green, not_found=amber, bond=neutral, error=red.
const STATUS_DOT: Record<string, string> = {
  ok: 'bg-pos-500', not_found: 'bg-warn-500', bond: 'bg-neutral-500', error: 'bg-neg-500',
};

type SortKey =
  | 'isin' | 'openfigi_figi' | 'openfigi_ticker' | 'openfigi_exch' | 'openfigi_type'
  | 'name' | 'analysis_symbol' | 'currency' | 'asset_class' | 'sector'
  | 'med_adv_eur' | 'price_from' | 'price_to' | 'volume_from' | 'volume_to' | 'zero_vol_frac';

// `sep` marks the first column of a group (ISIN | OpenFIGI | yfinance) → left
// border so the three families read as sections like the etoro table.
type Col = { key: SortKey; label: string; align?: 'right'; title?: string; sep?: boolean };
const COLS: Col[] = [
  { key: 'isin', label: 'ISIN' },
  { key: 'openfigi_figi', label: 'FIGI', sep: true, title: 'OpenFIGI — Bloomberg security identifier' },
  { key: 'openfigi_ticker', label: 'OF Ticker', title: 'OpenFIGI ticker(s) for this ISIN' },
  { key: 'openfigi_exch', label: 'OF Exch', title: 'OpenFIGI exchange code(s)' },
  { key: 'openfigi_type', label: 'OF Type', title: 'OpenFIGI security type' },
  { key: 'name', label: 'Name', sep: true },
  { key: 'analysis_symbol', label: 'Symbol', title: 'yfinance symbol we fetch the price series from — the ANALYSIS instrument (a wrapper like a BTC ETF maps to BTC-USD; the tradable listing is shown as "via …")' },
  { key: 'currency', label: 'Ccy' },
  { key: 'asset_class', label: 'Class' },
  { key: 'sector', label: 'Sector' },
  { key: 'med_adv_eur', label: '€ ADV', align: 'right', title: 'Median daily traded value in EUR (liquidity)' },
  { key: 'price_from', label: 'Price from', align: 'right', title: 'First stored price date' },
  { key: 'price_to', label: 'Price to', align: 'right', title: 'Last stored price date' },
  { key: 'volume_from', label: 'Vol from', align: 'right', title: 'First date with traded volume' },
  { key: 'volume_to', label: 'Vol to', align: 'right', title: 'Last date with traded volume' },
  { key: 'zero_vol_frac', label: 'Zero-vol %', align: 'right', title: 'Share of stored bars with zero volume — illiquidity / data-gap flag (a liquid equity ≈ 0%; FX/index ≈ 100%)' },
];

// Each column carries a coloured badge naming where its value came from: ISIN
// from the uploaded CSV, the openfigi_* block from OpenFIGI, everything else
// (incl. the parquet OHLCV) from yfinance/Yahoo.
const SOURCE_TONE: Record<string, string> = {
  csv: 'bg-accent-500/10 text-accent-300 border-accent-500/20',
  OpenFIGI: 'bg-warn-500/10 text-warn-300 border-warn-500/20',
  yfinance: 'bg-pos-500/10 text-pos-300 border-pos-500/20',
};
const _OPENFIGI_KEYS = new Set<SortKey>(['openfigi_figi', 'openfigi_ticker', 'openfigi_exch', 'openfigi_type']);
const sourceOf = (key: SortKey): keyof typeof SOURCE_TONE =>
  key === 'isin' ? 'csv' : _OPENFIGI_KEYS.has(key) ? 'OpenFIGI' : 'yfinance';

function SourceBadge({ source }: { source: keyof typeof SOURCE_TONE }) {
  return (
    <span className={`text-[8px] uppercase tracking-wider font-semibold px-1 py-0.5 rounded border ${SOURCE_TONE[source]}`}>
      {source}
    </span>
  );
}

/** Flat one-row-per-ISIN grid — the etoro-yfinance table, ISIN edition. ISIN →
 * OpenFIGI identity → yfinance columns. Searchable, filterable by class/status,
 * sortable; expand a mapped row for its chart; download the full-OHLCV parquet. */
export default function AssetPipelineTable({ reloadSignal }: { reloadSignal?: number }) {
  const [rows, setRows] = useState<AssetGridRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [chartRow, setChartRow] = useState<AssetGridRow | null>(null);

  const [q, setQ] = useState('');
  const [classFilter, setClassFilter] = useState('');
  const [sectorFilter, setSectorFilter] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [sort, setSort] = useState<{ key: SortKey; dir: 1 | -1 }>({ key: 'med_adv_eur', dir: -1 });

  const load = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/grid`);
      const body = await r.json().catch(() => null);
      if (!r.ok) setError(body?.detail ?? `HTTP ${r.status}`);
      else {
        // Dedupe by execution_id — offset pagination during a concurrent batch
        // insert can occasionally return a row twice; keep keys unique. (JS
        // Set.add returns the Set, not undefined, so use an explicit guard.)
        const seen = new Set<number>();
        const rows = ((body?.rows ?? []) as AssetGridRow[]).filter((r) => {
          if (seen.has(r.execution_id)) return false;
          seen.add(r.execution_id);
          return true;
        });
        setRows(rows);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);
  useEffect(() => { void load(); }, [load, reloadSignal]);

  // One predicate for every filter. `skip` omits a single facet so that facet's
  // option counts reflect all the OTHER active filters — faceted counts, so each
  // dropdown shows what picking an option would yield given the rest.
  const matches = useCallback((r: AssetGridRow, skip?: 'class' | 'sector' | 'status') => {
    if (skip !== 'class' && classFilter && r.asset_class !== classFilter) return false;
    if (skip !== 'sector' && sectorFilter && r.sector !== sectorFilter) return false;
    if (skip !== 'status' && statusFilter && r.status !== statusFilter) return false;
    const needle = q.trim().toLowerCase();
    if (needle) {
      const hay = `${r.isin} ${r.name ?? ''} ${r.yahoo_symbol ?? ''} ${r.analysis_symbol ?? ''} ${r.openfigi_ticker ?? ''} ${r.openfigi_figi ?? ''}`.toLowerCase();
      if (!hay.includes(needle)) return false;
    }
    return true;
  }, [q, classFilter, sectorFilter, statusFilter]);

  // Distinct values of `key` counted over rows matching all OTHER filters, most
  // common first. Always keeps the current selection selectable (count 0 if it
  // no longer matches). `total` = rows matching the other filters (the "All" count).
  const facet = useCallback((
    skip: 'class' | 'sector' | 'status',
    key: (r: AssetGridRow) => string | null | undefined,
    selected: string,
  ) => {
    const counts = new Map<string, number>();
    let total = 0;
    for (const r of rows ?? []) {
      if (!matches(r, skip)) continue;
      total++;
      const v = key(r);
      if (v) counts.set(v, (counts.get(v) ?? 0) + 1);
    }
    if (selected && !counts.has(selected)) counts.set(selected, 0);
    const opts = [...counts.entries()]
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .map(([value, count]) => ({ value, count }));
    return { opts, total };
  }, [rows, matches]);

  const classFacet = useMemo(() => facet('class', (r) => r.asset_class, classFilter), [facet, classFilter]);
  const sectorFacet = useMemo(() => facet('sector', (r) => r.sector, sectorFilter), [facet, sectorFilter]);
  const statusFacet = useMemo(() => facet('status', (r) => r.status, statusFilter), [facet, statusFilter]);

  const view = useMemo(() => {
    const { key, dir } = sort;
    return (rows ?? []).filter((r) => matches(r)).sort((a, b) => {
      const av = a[key], bv = b[key];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;      // nulls last, regardless of dir
      if (bv == null) return -1;
      if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * dir;
      return String(av).localeCompare(String(bv)) * dir;
    });
  }, [rows, matches, sort]);

  const clickSort = (key: SortKey) =>
    setSort((s) => (s.key === key ? { key, dir: (s.dir === 1 ? -1 : 1) as 1 | -1 } : { key, dir: 1 }));

  // Row virtualization — only the rows in view get mounted (~40 instead of
  // thousands), so sort/filter/scroll stay instant no matter how many assets.
  const scrollRef = useRef<HTMLDivElement>(null);
  const rowVirtualizer = useVirtualizer<HTMLDivElement, HTMLTableRowElement>({
    count: view.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 34,
    overscan: 12,
  });
  const vItems = rowVirtualizer.getVirtualItems();
  const padTop = vItems.length ? vItems[0].start : 0;
  const padBottom = vItems.length ? rowVirtualizer.getTotalSize() - vItems[vItems.length - 1].end : 0;

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <h3 className="text-sm font-semibold text-fg-strong">
          Execution instruments{rows ? ` · ${view.length}/${rows.length}` : ''}
        </h3>
        <div className="flex items-center gap-2 flex-wrap">
          <input
            value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search ISIN / name / symbol / FIGI…"
            className="bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 w-60"
          />
          <select value={classFilter} onChange={(e) => setClassFilter(e.target.value)}
            className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30">
            <option value="">All classes ({classFacet.total})</option>
            {classFacet.opts.map((o) => <option key={o.value} value={o.value}>{o.value} ({o.count})</option>)}
          </select>
          <select value={sectorFilter} onChange={(e) => setSectorFilter(e.target.value)}
            className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 max-w-[180px]">
            <option value="">All sectors ({sectorFacet.total})</option>
            {sectorFacet.opts.map((o) => <option key={o.value} value={o.value}>{o.value} ({o.count})</option>)}
          </select>
          <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}
            className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30">
            <option value="">All statuses ({statusFacet.total})</option>
            {statusFacet.opts.map((o) => <option key={o.value} value={o.value}>{o.value} ({o.count})</option>)}
          </select>
          <button type="button" onClick={() => void load()} disabled={loading}
            className="text-xs px-3 py-1.5 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 hover:border-accent-500/50 disabled:opacity-40 transition-colors">
            {loading ? 'Loading…' : 'Refresh'}
          </button>
          <button type="button" onClick={() => { setQ(''); setClassFilter(''); setSectorFilter(''); setStatusFilter(''); }}
            disabled={!q.trim() && !classFilter && !sectorFilter && !statusFilter}
            className="text-xs px-3 py-1.5 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 hover:border-accent-500/50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
            Clear filters
          </button>
        </div>
      </div>

      {error && <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>}
      {rows && rows.length === 0 && <div className="text-xs text-fg-subtle">Nothing ingested yet — upload a CSV and run a batch above.</div>}

      {rows && rows.length > 0 && (
        <div ref={scrollRef} className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[70vh]">
          <table className="w-full text-xs">
            <thead className="bg-card sticky top-0 z-10">
              <tr className="bg-card text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40 align-top">
                {COLS.map((c) => (
                  <th key={c.key} title={c.title}
                    onClick={() => clickSort(c.key)}
                    className={`px-3 py-1.5 font-medium cursor-pointer select-none whitespace-nowrap hover:text-fg-soft ${c.sep ? 'border-l border-neutral-800/40' : ''}`}>
                    <div className={`flex flex-col gap-1 ${c.align === 'right' ? 'items-end' : 'items-start'}`}>
                      <span>{c.label}{sort.key === c.key && <span className="text-accent-400 ml-0.5">{sort.dir === 1 ? '▲' : '▼'}</span>}</span>
                      <SourceBadge source={sourceOf(c.key)} />
                    </div>
                  </th>
                ))}
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap border-l border-neutral-800/40" title="Native + EUR price/volume charts">Chart</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {padTop > 0 && <tr aria-hidden><td colSpan={COLS.length + 1} style={{ height: padTop }} /></tr>}
              {vItems.map((vi) => {
                const r = view[vi.index];
                return (
                  <GridRow key={r.execution_id} r={r} onChart={() => setChartRow(r)}
                    measureRef={rowVirtualizer.measureElement} dataIndex={vi.index} />
                );
              })}
              {padBottom > 0 && <tr aria-hidden><td colSpan={COLS.length + 1} style={{ height: padBottom }} /></tr>}
            </tbody>
          </table>
        </div>
      )}

      {chartRow && <AssetChartModal row={chartRow} onClose={() => setChartRow(null)} />}
    </section>
  );
}

function GridRow({ r, onChart, measureRef, dataIndex }: {
  r: AssetGridRow;
  onChart: () => void;
  measureRef?: (el: HTMLTableRowElement | null) => void;
  dataIndex?: number;
}) {
  const hasSeries = r.analysis_id != null && (r.bars ?? 0) > 0;
  return (
      <tr ref={measureRef} data-index={dataIndex} className="hover:bg-accent-500/10 transition-colors">
        <td className="px-3 py-1.5 font-mono text-fg whitespace-nowrap">
          <span title={`${r.status}${r.reason ? ' — ' + r.reason : ''}`}
            className={`inline-block h-2 w-2 rounded-full align-middle mr-2 ${STATUS_DOT[r.status] ?? 'bg-neutral-500'}`} />
          {r.isin}
        </td>
        {/* OpenFIGI group */}
        <td className="px-3 py-1.5 font-mono text-fg-subtle whitespace-nowrap border-l border-neutral-800/40">
          <span className="inline-block max-w-[120px] truncate align-bottom" title={r.openfigi_figi ?? ''}>{r.openfigi_figi ?? '—'}</span>
        </td>
        <td className="px-3 py-1.5 font-mono text-fg-muted whitespace-nowrap">
          <span className="inline-block max-w-[110px] truncate align-bottom" title={r.openfigi_ticker ?? ''}>{r.openfigi_ticker ?? '—'}</span>
        </td>
        <td className="px-3 py-1.5 font-mono text-fg-subtle whitespace-nowrap">
          <span className="inline-block max-w-[90px] truncate align-bottom" title={r.openfigi_exch ?? ''}>{r.openfigi_exch ?? '—'}</span>
        </td>
        <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">
          <span className="inline-block max-w-[110px] truncate align-bottom" title={r.openfigi_type ?? ''}>{r.openfigi_type ?? '—'}</span>
        </td>
        {/* yfinance group */}
        <td className="px-3 py-1.5 text-fg-soft border-l border-neutral-800/40">
          <span className="inline-block max-w-[200px] truncate align-bottom" title={r.name ?? ''}>{r.name ?? '—'}</span>
        </td>
        <td className="px-3 py-1.5 font-mono text-fg whitespace-nowrap">
          {r.analysis_symbol ? (
            <>
              <a href={`https://finance.yahoo.com/quote/${encodeURIComponent(r.analysis_symbol)}`}
                target="_blank" rel="noreferrer" onClick={(e) => e.stopPropagation()}
                className="text-accent-400 hover:underline">{r.analysis_symbol}</a>
              {r.yahoo_symbol && r.yahoo_symbol !== r.analysis_symbol && (
                <span className="ml-1.5 text-[10px] text-fg-faint" title="Tradable listing (execution)">via {r.yahoo_symbol}</span>
              )}
            </>
          ) : (r.yahoo_symbol ?? '—')}
        </td>
        <td className="px-3 py-1.5 font-mono text-fg-muted whitespace-nowrap">{r.currency ?? '—'}</td>
        <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">{r.asset_class ?? '—'}</td>
        <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">
          <span className="inline-block max-w-[140px] truncate align-bottom" title={r.sector ?? ''}>{r.sector ?? '—'}</span>
        </td>
        <td className="px-3 py-1.5 text-right font-mono text-fg whitespace-nowrap">{adv(r.med_adv_eur)}</td>
        <td className="px-3 py-1.5 text-right font-mono text-fg-subtle whitespace-nowrap">{r.price_from ?? '—'}</td>
        <td className="px-3 py-1.5 text-right font-mono text-fg-subtle whitespace-nowrap">{r.price_to ?? '—'}</td>
        <td className="px-3 py-1.5 text-right font-mono text-fg-subtle whitespace-nowrap">{r.volume_from ?? '—'}</td>
        <td className="px-3 py-1.5 text-right font-mono text-fg-subtle whitespace-nowrap">{r.volume_to ?? '—'}</td>
        <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
          {r.zero_vol_frac == null
            ? <span className="text-fg-faint">—</span>
            : <span className={r.zero_vol_frac >= 0.05 ? 'text-warn-300' : 'text-fg-subtle'}>{(r.zero_vol_frac * 100).toFixed(1)}%</span>}
        </td>
        {/* Chart — native + EUR price/volume modal (last cell) */}
        <td className="px-3 py-1.5 whitespace-nowrap border-l border-neutral-800/40">
          {hasSeries ? (
            <button type="button" onClick={onChart} title="View native + EUR price & volume charts"
              className="text-[10px] px-2 py-0.5 rounded border border-neutral-700 text-accent-400 hover:border-accent-500/50 transition-colors">
              Chart
            </button>
          ) : <span className="text-fg-faint">—</span>}
        </td>
      </tr>
  );
}
