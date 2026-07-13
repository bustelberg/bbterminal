'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import type { AssetGridRow, DividendCoverageEntry } from '../../lib/types/api';
import { classLabel, sectorLabel } from '../../lib/assetLabels';
import AssetChartModal from './AssetChartModal';
import AssetDividendModal from './AssetDividendModal';
import AssetFinancialModal, { type LineItem } from './AssetFinancialModal';
import RowResolveModal from './RowResolveModal';
import CreateUniverseModal from './CreateUniverseModal';

/** Median daily traded value, EUR — compact. */
const adv = (v: number | null | undefined) =>
  v == null ? '—' : v >= 1e9 ? `€${(v / 1e9).toFixed(2)}B` : v >= 1e6 ? `€${(v / 1e6).toFixed(1)}M` : v >= 1e3 ? `€${(v / 1e3).toFixed(0)}k` : `€${v.toFixed(0)}`;

/** Market cap, EUR — compact (€T/€B/€M). */
const mcap = (v: number | null | undefined) =>
  v == null ? '—' : v >= 1e12 ? `€${(v / 1e12).toFixed(2)}T` : v >= 1e9 ? `€${(v / 1e9).toFixed(1)}B` : v >= 1e6 ? `€${(v / 1e6).toFixed(0)}M` : `€${v.toFixed(0)}`;

// Resolution status → dot colour: ok=green, not_found=amber, bond=neutral,
// error=red, queued=blue (awaiting the background worker — not yet resolved).
const STATUS_DOT: Record<string, string> = {
  ok: 'bg-pos-500', not_found: 'bg-warn-500', bond: 'bg-neutral-500', error: 'bg-neg-500',
  queued: 'bg-accent-500',
};

type SortKey =
  | 'isin'
  | 'leonteq_name' | 'leonteq_product_type' | 'leonteq_currency'
  | 'openfigi_name' | 'identity_status'
  | 'name' | 'analysis_symbol' | 'currency' | 'asset_class' | 'sector'
  | 'country' | 'continent' | 'msci_region'
  | 'med_adv_eur' | 'market_cap_eur' | 'price_from' | 'price_to' | 'volume_from' | 'volume_to' | 'zero_vol_frac';

// The facets the toolbar offers. Order here is the order they render; the
// geography trio (country → continent → region) reads outward from the most
// specific, and sits directly after Sector.
type FacetKey = 'class' | 'sector' | 'product' | 'country' | 'continent' | 'region';

// `sep` marks the first column of a group (ISIN | Leonteq | OpenFIGI | yfinance)
// → left border so the families read as sections.
type Col = { key: SortKey; label: string; align?: 'right'; title?: string; sep?: boolean };
const COLS: Col[] = [
  { key: 'isin', label: 'ISIN' },
  { key: 'leonteq_name', label: 'Ltq Name', title: 'Name from the uploaded Leonteq (lynqs) list' },
  { key: 'leonteq_product_type', label: 'Product', title: 'Leonteq productType (EQUITY, ETF, …)' },
  { key: 'leonteq_currency', label: 'Ltq Ccy', title: 'Currency from the Leonteq list' },
  { key: 'openfigi_name', label: 'OpenFIGI Name', sep: true, title: 'Independent instrument name from OpenFIGI (the identity cross-check)' },
  { key: 'identity_status', label: 'Match', title: 'Does the OpenFIGI name confirm the resolved yfinance instrument? verified = yes · mismatch = OpenFIGI names a different company (likely wrong resolution)' },
  { key: 'name', label: 'Name', sep: true },
  { key: 'analysis_symbol', label: 'Symbol', title: 'yfinance symbol we fetch the price series from — the ANALYSIS instrument (a wrapper like a BTC ETF maps to BTC-USD; the tradable listing is shown as "via …")' },
  { key: 'currency', label: 'Ccy' },
  { key: 'asset_class', label: 'Class' },
  { key: 'sector', label: 'Sector' },
  { key: 'country', label: 'Country', title: 'Issuer domicile (Yahoo assetProfile), falling back to the listing venue when unknown. An ETF/crypto has no domicile, so it shows its LISTING country.' },
  { key: 'continent', label: 'Continent', title: 'Geographic continent of the country. Israel → Asia, Turkey → Asia (unlike the MSCI region).' },
  { key: 'msci_region', label: 'Region', title: 'MSCI ACWI region: North America · Europe · Pacific · Emerging Markets. Financial, not geographic — Israel is Europe, South Korea and Taiwan are Emerging Markets. Blank = the country has no MSCI market.' },
  { key: 'med_adv_eur', label: '€ ADV', align: 'right', title: 'Median daily traded value in EUR (liquidity of THIS listing)' },
  { key: 'market_cap_eur', label: '€ Mkt Cap', align: 'right', title: 'Market cap in EUR — from the company PRIMARY listing, so listing-independent (a stranded thin listing still shows the true size)' },
  { key: 'price_from', label: 'Price from', align: 'right', title: 'First stored price date' },
  { key: 'price_to', label: 'Price to', align: 'right', title: 'Last stored price date' },
  { key: 'volume_from', label: 'Vol from', align: 'right', title: 'First date with traded volume' },
  { key: 'volume_to', label: 'Vol to', align: 'right', title: 'Last date with traded volume' },
  { key: 'zero_vol_frac', label: 'Zero-vol %', align: 'right', title: 'Share of stored bars with zero volume — illiquidity / data-gap flag (a liquid equity ≈ 0%; FX/index ≈ 100%)' },
];

// Each column carries a coloured badge naming where its value came from: ISIN +
// the Leonteq metadata from the uploaded list, the OpenFIGI name + Match verdict
// from OpenFIGI, everything else (incl. the parquet OHLCV) from yfinance/Yahoo.
const SOURCE_TONE: Record<string, string> = {
  Leonteq: 'bg-accent-600/15 text-accent-400 border-accent-600/30',
  OpenFIGI: 'bg-warn-500/10 text-warn-300 border-warn-500/20',
  yfinance: 'bg-pos-500/10 text-pos-300 border-pos-500/20',
  // Every other column comes from the Yahoo/OpenFIGI/Leonteq pipeline keyed by
  // ISIN. The trailing group (Exchange · Ticker · Div/share · Revenue) is the only
  // one sourced from GuruFocus, reached by bridging this row's ISIN to a GuruFocus
  // listing — so it earns its own badge. Exchange + Ticker ARE the two halves of the
  // `EXCHANGE:TICKER` symbol both fetches query, which is why they lead the group.
  //
  // Neutral slate, NOT `neg-*`: the three semantic ramps (accent/warn/pos) are
  // taken by the badges above, and `neg-*` means "negative return / error" in this
  // palette. A red source badge reads as a broken column.
  GuruFocus: 'bg-overlay/[0.06] text-fg-muted border-neutral-700',
};
const _OPENFIGI_KEYS = new Set<SortKey>(['openfigi_name', 'identity_status']);
// ISIN + the Leonteq metadata columns all come from the uploaded Leonteq list.
const _LEONTEQ_KEYS = new Set<SortKey>(['isin', 'leonteq_name', 'leonteq_product_type', 'leonteq_currency']);
const sourceOf = (key: SortKey): keyof typeof SOURCE_TONE =>
  _LEONTEQ_KEYS.has(key) ? 'Leonteq' : _OPENFIGI_KEYS.has(key) ? 'OpenFIGI' : 'yfinance';

// Match badge: verified = green ✓, mismatch = amber ⚠, else neutral —.
const MATCH_TONE: Record<string, string> = {
  verified: 'bg-pos-500/10 text-pos-400 border-pos-500/20',
  mismatch: 'bg-warn-500/10 text-warn-400 border-warn-500/20',
};
// Shown in a source's cell when that source was requested but found nothing.
function MissingBadge() {
  return (
    <span title="Requested but this source found nothing"
      className="text-[9px] uppercase tracking-wider font-semibold px-1.5 py-0.5 rounded border bg-neg-500/10 text-neg-400 border-neg-500/20">
      missing
    </span>
  );
}

function MatchBadge({ status }: { status?: string | null }) {
  if (status === 'verified')
    return <span title="OpenFIGI confirms the resolved instrument" className={`text-[9px] uppercase tracking-wider font-semibold px-1.5 py-0.5 rounded border ${MATCH_TONE.verified}`}>✓ Match</span>;
  if (status === 'mismatch')
    return <span title="OpenFIGI names a different company — likely wrong resolution" className={`text-[9px] uppercase tracking-wider font-semibold px-1.5 py-0.5 rounded border ${MATCH_TONE.mismatch}`}>⚠ Mismatch</span>;
  return <span className="text-fg-faint" title="No OpenFIGI name to compare">—</span>;
}

function SourceBadge({ source }: { source: keyof typeof SOURCE_TONE }) {
  return (
    <span className={`text-[8px] uppercase tracking-wider font-semibold px-1 py-0.5 rounded border ${SOURCE_TONE[source]}`}>
      {source}
    </span>
  );
}

/** Flat one-row-per-ISIN grid — the instrument table, ISIN edition. ISIN →
 * OpenFIGI identity → yfinance columns. Searchable, filterable by class/status,
 * sortable; expand a mapped row for its chart; download the full-OHLCV parquet. */
export default function AssetPipelineTable({ reloadSignal }: { reloadSignal?: number }) {
  const [rows, setRows] = useState<AssetGridRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [chartRow, setChartRow] = useState<AssetGridRow | null>(null);
  const [resolveRow, setResolveRow] = useState<AssetGridRow | null>(null);
  // ISIN -> GuruFocus company. Only ~13% of grid rows resolve; the rest are ETFs,
  // crypto, or equities never ingested into `company`. An ISIN absent from this
  // map has no obtainable dividend data, and the cell says so rather than
  // rendering a blank that reads as "pays no dividend".
  const [dividends, setDividends] = useState<Record<string, DividendCoverageEntry>>({});
  const [dividendRow, setDividendRow] = useState<AssetGridRow | null>(null);
  // Which row + which income-statement line the modal is showing. One modal serves every
  // line item, so a third column is a registry entry, not another piece of state.
  const [financialRow, setFinancialRow] = useState<{ row: AssetGridRow; item: LineItem } | null>(null);

  const [q, setQ] = useState('');
  const [classFilter, setClassFilter] = useState('');
  const [sectorFilter, setSectorFilter] = useState('');
  const [productFilter, setProductFilter] = useState('');
  const [countryFilter, setCountryFilter] = useState('');
  const [continentFilter, setContinentFilter] = useState('');
  const [regionFilter, setRegionFilter] = useState('');
  const [sort, setSort] = useState<{ key: SortKey; dir: 1 | -1 }>({ key: 'med_adv_eur', dir: -1 });

  // Saved universes: filter the grid to a universe's member tickers + create new.
  const [universes, setUniverses] = useState<{ id: number; name: string; ticker_count: number }[]>([]);
  const [universeFilter, setUniverseFilter] = useState('');            // universe id (string)
  const [universeMembers, setUniverseMembers] = useState<Set<string> | null>(null);
  const [showCreate, setShowCreate] = useState(false);

  const loadUniverses = useCallback(async () => {
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/universes`);
      const b = await r.json().catch(() => null);
      if (r.ok) setUniverses(b?.universes ?? []);
    } catch { /* ignore */ }
  }, []);
  useEffect(() => { void loadUniverses(); }, [loadUniverses]);

  // One small map (~2.5k entries), joined to the grid on ISIN client-side. Failing
  // to load it must leave the column inert, never break the table.
  const loadDividendCoverage = useCallback(async () => {
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/dividends/coverage`);
      const b = await r.json().catch(() => null);
      if (r.ok) setDividends(b?.by_isin ?? {});
    } catch { /* the column degrades to "—"; the grid is unaffected */ }
  }, []);
  useEffect(() => { void loadDividendCoverage(); }, [loadDividendCoverage]);

  // Fetch the selected universe's member tickers (for the grid filter).
  useEffect(() => {
    if (!universeFilter) { setUniverseMembers(null); return; }
    let alive = true;
    (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/asset-pipeline/universes/${universeFilter}/members`);
        const b = await r.json().catch(() => null);
        if (alive && r.ok) setUniverseMembers(new Set<string>(b?.members ?? []));
      } catch { /* ignore */ }
    })();
    return () => { alive = false; };
  }, [universeFilter]);

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
  const matches = useCallback((r: AssetGridRow, skip?: FacetKey) => {
    if (skip !== 'class' && classFilter && r.asset_class !== classFilter) return false;
    if (skip !== 'sector' && sectorFilter && r.sector !== sectorFilter) return false;
    if (skip !== 'product' && productFilter && r.leonteq_product_type !== productFilter) return false;
    if (skip !== 'country' && countryFilter && r.country !== countryFilter) return false;
    if (skip !== 'continent' && continentFilter && r.continent !== continentFilter) return false;
    if (skip !== 'region' && regionFilter && r.msci_region !== regionFilter) return false;
    if (universeFilter && universeMembers && !universeMembers.has(r.analysis_symbol ?? '')) return false;
    const needle = q.trim().toLowerCase();
    if (needle) {
      const hay = `${r.isin} ${r.name ?? ''} ${r.leonteq_name ?? ''} ${r.openfigi_name ?? ''} ${r.yahoo_symbol ?? ''} ${r.analysis_symbol ?? ''} ${r.country ?? ''}`.toLowerCase();
      if (!hay.includes(needle)) return false;
    }
    return true;
  }, [q, classFilter, sectorFilter, productFilter, countryFilter, continentFilter, regionFilter, universeFilter, universeMembers]);

  // Distinct values of `key` counted over rows matching all OTHER filters, most
  // common first. Always keeps the current selection selectable (count 0 if it
  // no longer matches). `total` = rows matching the other filters (the "All" count).
  const facet = useCallback((
    skip: FacetKey,
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
  const productFacet = useMemo(() => facet('product', (r) => r.leonteq_product_type, productFilter), [facet, productFilter]);
  const countryFacet = useMemo(() => facet('country', (r) => r.country, countryFilter), [facet, countryFilter]);
  const continentFacet = useMemo(() => facet('continent', (r) => r.continent, continentFilter), [facet, continentFilter]);
  const regionFacet = useMemo(() => facet('region', (r) => r.msci_region, regionFilter), [facet, regionFilter]);

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
        <div className="flex items-end gap-2 flex-wrap">
          <input
            value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search ISIN / name / symbol / FIGI…"
            className="bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 w-60"
          />
          {/* Leonteq PRODUCT filter (Leonteq badge) */}
          <div className="flex flex-col items-start gap-1">
            <SourceBadge source="Leonteq" />
            <select value={productFilter} onChange={(e) => setProductFilter(e.target.value)}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 max-w-[160px]" title="Leonteq productType">
              <option value="">All products ({productFacet.total})</option>
              {productFacet.opts.map((o) => <option key={o.value} value={o.value}>{o.value} ({o.count})</option>)}
            </select>
          </div>
          {/* yfinance CLASS filter (yfinance badge) */}
          <div className="flex flex-col items-start gap-1">
            <SourceBadge source="yfinance" />
            <select value={classFilter} onChange={(e) => setClassFilter(e.target.value)}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30" title="yfinance asset class">
              <option value="">All classes ({classFacet.total})</option>
              {classFacet.opts.map((o) => <option key={o.value} value={o.value}>{classLabel(o.value)} ({o.count})</option>)}
            </select>
          </div>
          {/* yfinance SECTOR filter (yfinance badge) */}
          <div className="flex flex-col items-start gap-1">
            <SourceBadge source="yfinance" />
            <select value={sectorFilter} onChange={(e) => setSectorFilter(e.target.value)}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 max-w-[180px]" title="yfinance sector">
              <option value="">All sectors ({sectorFacet.total})</option>
              {sectorFacet.opts.map((o) => <option key={o.value} value={o.value}>{sectorLabel(o.value)} ({o.count})</option>)}
            </select>
          </div>
          {/* Geography trio — country → continent → region, widening outward.
              Each is faceted against the others, so picking Japan narrows the
              continent list to Asia rather than offering empty options. */}
          <div className="flex flex-col items-start gap-1">
            <SourceBadge source="yfinance" />
            <select value={countryFilter} onChange={(e) => setCountryFilter(e.target.value)}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 max-w-[180px]" title="Issuer domicile (falls back to the listing venue)">
              <option value="">All countries ({countryFacet.total})</option>
              {countryFacet.opts.map((o) => <option key={o.value} value={o.value}>{o.value} ({o.count})</option>)}
            </select>
          </div>
          <div className="flex flex-col items-start gap-1">
            <SourceBadge source="yfinance" />
            <select value={continentFilter} onChange={(e) => setContinentFilter(e.target.value)}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 max-w-[180px]" title="Geographic continent">
              <option value="">All continents ({continentFacet.total})</option>
              {continentFacet.opts.map((o) => <option key={o.value} value={o.value}>{o.value} ({o.count})</option>)}
            </select>
          </div>
          <div className="flex flex-col items-start gap-1">
            <SourceBadge source="yfinance" />
            <select value={regionFilter} onChange={(e) => setRegionFilter(e.target.value)}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 max-w-[180px]" title="MSCI ACWI region">
              <option value="">All regions ({regionFacet.total})</option>
              {regionFacet.opts.map((o) => <option key={o.value} value={o.value}>{o.value} ({o.count})</option>)}
            </select>
          </div>
          {/* Universe membership filter */}
          <select value={universeFilter} onChange={(e) => setUniverseFilter(e.target.value)} title="Filter to a saved universe's tickers"
            className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 max-w-[200px]">
            <option value="">All universes</option>
            {universes.map((u) => <option key={u.id} value={u.id}>{u.name} ({u.ticker_count.toLocaleString()})</option>)}
          </select>
          <button type="button" onClick={() => { setQ(''); setClassFilter(''); setSectorFilter(''); setProductFilter(''); setCountryFilter(''); setContinentFilter(''); setRegionFilter(''); setUniverseFilter(''); }}
            disabled={!q.trim() && !classFilter && !sectorFilter && !productFilter && !countryFilter && !continentFilter && !regionFilter && !universeFilter}
            className="text-xs px-3 py-1.5 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 hover:border-accent-500/50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
            Clear filters
          </button>
          <button type="button" onClick={() => setShowCreate(true)}
            className="text-xs px-3 py-1.5 rounded-lg bg-accent-600 hover:bg-accent-500 text-white transition-colors">
            + Create universe
          </button>
        </div>
      </div>

      {/* Indeterminate loading bar — shows while the grid loads (one bulk fetch,
          so there's no % to report). */}
      {loading && <div className="loading-bar h-0.5 w-full rounded-full" aria-hidden />}

      {error && <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>}
      {rows === null && loading && <div className="text-xs text-fg-subtle">Loading instruments…</div>}
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
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap border-l border-neutral-800/40" title="Chart · manual OpenFIGI + yfinance resolve">Actions</th>
                {/* GuruFocus group — the resolved listing, then its dividends. The
                    separator sits on Exchange (the group's first column), so the
                    three read as one section. */}
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap border-l border-neutral-800/40"
                  title="GuruFocus exchange code of the listing this ISIN resolves to — the EXCHANGE half of the EXCHANGE:TICKER symbol the Div/share fetch queries. Blank until the row is resolved (hit Fetch).">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Exchange</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="GuruFocus ticker of the listing this ISIN resolves to. It can differ from the yfinance Symbol — separate id spaces, joined only by ISIN, and GuruFocus may hold a different listing of the same security.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Ticker</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Cash paid per unit held (GuruFocus). The same series for a stock and an ETF: (date, cash per unit), charted in native currency and EUR.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Div/share</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Revenue (GuruFocus), in MILLIONS of the listing's trading currency + EUR. Only an operating business has one — bonds, futures and funds don't (a fund holds securities, it doesn't trade). NOTE: GuruFocus converts financials into the LISTING's currency per fiscal period, so a non-home listing reports a different number (CSX: 14,092 USD on Nasdaq vs 12,034.6 EUR on Xetra for FY2025).">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Revenue</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Gross profit (GuruFocus), in MILLIONS of the listing's trading currency + EUR. Same financials blob as Revenue, so opening the second column on a company costs no extra API call. NOTE: a BANK has no gross profit line at all — no cost of goods sold — so JPMorgan reports N/A rather than an empty chart.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Gross profit</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="EBIT (GuruFocus), in MILLIONS of the listing's trading currency + EUR. GuruFocus's OWN EBIT line — not Operating Income, which is a different number (Mitsui Chemicals: EBIT 85,035 vs Operating Income 56,602). Same blob as Revenue/Gross profit, so no extra API call. A BANK has no EBIT line either → N/A.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>EBIT</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Interest expense (GuruFocus), in MILLIONS of the listing's trading currency + EUR. Reported NEGATIVE — it is an outflow — and charted as reported, never sign-flipped (Apple −3,933 · JPMorgan −101,350). A 0 is a real value, not a missing period: Apple's last two years net it out. Unlike Gross profit and EBIT, a BANK DOES have this line — interest expense is a bank's core cost.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Interest exp.</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Pretax income (GuruFocus), in MILLIONS of the listing's trading currency + EUR. Present in EVERY industry template — JPMorgan reports it (75,081) despite having no EBIT and no gross profit. NOT the same as EBIT: EBIT is before interest, pretax is after it (Mitsui Chemicals FY2026: EBIT 85,035 vs Pretax 68,608 — the gap is the interest bill).">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Pretax income</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Income tax (GuruFocus), in MILLIONS of the listing's trading currency + EUR. GuruFocus calls this line 'Tax Provision' — there is no 'Income Tax' key. Reported NEGATIVE (an outflow: Apple −20,719 · JPMorgan −16,610) and charted as reported, never sign-flipped. Present in every template, banks included.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Income tax</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Net income (GuruFocus), in MILLIONS of the listing's trading currency + EUR. The bottom line ATTRIBUTABLE TO SHAREHOLDERS — what EPS is built from. NOTE: pretax + tax will NOT tie to this for a company with minority interests, and that is correct. Mitsui Chemicals FY2025: pretax 68,608 + tax −21,698 = 46,910 = 'Net Income Including Noncontrolling Interests', while Net Income = 34,378; the 12,532 gap is minority interest.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Net income</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Depreciation & amortization (GuruFocus 'Depreciation, Depletion and Amortization'), in MILLIONS of the listing's trading currency + EUR. Reported POSITIVE (a magnitude, not an outflow): Apple 11,698 · JPMorgan 8,821 · Mitsui 104,744. Present in every template, banks included. Read from the INCOME statement; the cashflow statement carries an identical twin ('Cash Flow Depreciation…') that we never touch.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>D&amp;A</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Diluted EPS (GuruFocus 'EPS (Diluted)') — the ONLY column here that is PER SHARE, not millions: Apple 7.46 USD/share · JPMorgan 20.02 · Mitsui 91.62 JPY/share. It ties out (Apple: net income 112,010M ÷ 15,004.7M diluted shares = 7.46). Still a currency amount, so the EUR panel converts at each period-end rate exactly as elsewhere. Diluted, not basic — the conservative share count. Present in every template, banks included.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>EPS (dil.)</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Operating cash flow (GuruFocus 'Cash Flow from Operations'), in MILLIONS of the listing's trading currency + EUR. Read from the CASHFLOW statement, not the income statement. Its SIGN is real information, not a convention: Apple +111,482 but JPMorgan −147,782 — a bank's operating cash flow routinely goes negative as loans and trading assets grow. A negative line here is an answer, not a bug.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Operating CF</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Capex (GuruFocus 'Capital Expenditure'), from the CASHFLOW statement, in MILLIONS + EUR. Reported NEGATIVE — an outflow — and charted as reported. NOT the same as 'Purchase Of Property, Plant, Equipment': capex also picks up intangibles (Mitsui: PP&E −128,242 vs capex −137,759). The mapping ties out: Apple OCF 111,482 + capex −12,715 = 98,767 = GuruFocus's own Free Cash Flow. JPMorgan reports 0 — a real value, not N/A; a bank's capex is negligible here.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Capex</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Total debt — DERIVED, in MILLIONS + EUR. GuruFocus publishes no 'Total Debt' line, so this is short-term + long-term debt from the BALANCE SHEET (each incl. capital lease obligations). Apple 20,329 + 78,328 = 98,657 · JPMorgan 68,048 + 448,764 = 516,812. The capital-lease variants are used because they are the only keys a BANK has (JPMorgan lacks the plain Short-/Long-Term Debt keys), and for other companies they are identical to the plain ones. A period where one component is missing is dropped, never summed as if it were zero — an understated debt that looks like a real number is worse than a gap.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Total debt</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Cash & equivalents, from the BALANCE SHEET, in MILLIONS + EUR. GuruFocus RENAMES this line per industry template, so two spellings are coalesced (not summed): 'Cash and Cash Equivalents' for most companies, 'Balance Statement Cash and cash equivalents' for a BANK — JPMorgan has neither of the ordinary keys, so mapping only the first would N/A every bank. NOTE this is the NARROW line: it is NOT 'Cash, Cash Equivalents, Marketable Securities', which is a different and much larger number (Apple: 54,697 vs cash-only 35,934).">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Cash &amp; equiv.</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Shareholders' equity (GuruFocus 'Total Stockholders Equity'), from the BALANCE SHEET, in MILLIONS + EUR. The SHAREHOLDERS' line — what an equity holder owns, and the denominator of book value per share and ROE. NOT 'Total Equity', which includes minority interest: Mitsui Chemicals reports Total Stockholders Equity 864,727 + Minority Interest 124,057 = Total Equity 988,784. JPMorgan's two are identical (362,438, no minorities), so checking there alone would bless either choice.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Equity</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Shares outstanding (GuruFocus 'Shares Outstanding (Diluted Average)'), in MILLIONS OF SHARES: Apple 15,004.7 (= 15.0bn) · JPMorgan 2,781.5 · Mitsui 375.2. This is a COUNT, not currency — so there is NO EUR chart: dividing a number of shares by an FX rate would mean nothing. It's the diluted average, the same basis as EPS, which is what makes them tie (Apple: 7.46 × 15,004.7 ≈ net income 112,010).">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Shares out.</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Forward EPS — analyst CONSENSUS estimate, per share, NOT a reported result. From GuruFocus's 'analyst_estimate' endpoint (SINGULAR — the plural is a fake endpoint that 200s with junk), a different source from every other column here, which read the reported financials. Apple: FY2026-09 8.76 rising to 14.43 by FY2030. Every date is in the FUTURE, so no FX rate exists for it — the EUR line converts at the LATEST KNOWN rate, which is the only honest choice but means it carries today's FX assumption. An uncovered company has no estimates at all.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Forward EPS</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Estimated revenue growth — DERIVED, in PERCENT (not currency, so there is no EUR chart). GuruFocus's own 'future_revenue_estimate_growth' is a single long-term SCALAR (Apple 10.09, NVIDIA 45.73), not a series — nothing to plot. So this is the year-over-year change in the consensus REVENUE estimates, with the first forecast year measured against the last REPORTED revenue (Apple: est FY2026 477,600 vs actual FY2025 416,161 = +14.8%). It will NOT equal GuruFocus's scalar: theirs is a long-run average, this is the actual year-on-year step the consensus implies.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Rev growth (est)</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap"
                  title="Long-term EPS growth — analyst CONSENSUS, in PERCENT. A SINGLE FIGURE, not a series: GuruFocus publishes one number (Apple 13.01 · NVIDIA 47.57 · JPMorgan 8.66), so there is nothing to chart and the modal shows the number. It is a forecast of the growth RATE, not of earnings. NOT 'future_per_share_eps_estimate_growth' (13.03 / 45.72 / 8.23 — the CAGR implied by the estimate series), which is close enough on Apple to be mistaken for it and diverges on NVIDIA.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>EPS LTG (est)</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {/* +22: Actions + the twenty-one GuruFocus columns live outside COLS. */}
              {padTop > 0 && <tr aria-hidden><td colSpan={COLS.length + 22} style={{ height: padTop }} /></tr>}
              {vItems.map((vi) => {
                const r = view[vi.index];
                return (
                  <GridRow key={r.execution_id} r={r} onChart={() => setChartRow(r)}
                    onResolve={() => setResolveRow(r)}
                    dividend={r.isin ? dividends[r.isin] : undefined}
                    onDividends={() => setDividendRow(r)}
                    onFinancial={(item) => setFinancialRow({ row: r, item })}
                    measureRef={rowVirtualizer.measureElement} dataIndex={vi.index} />
                );
              })}
              {padBottom > 0 && <tr aria-hidden><td colSpan={COLS.length + 4} style={{ height: padBottom }} /></tr>}
            </tbody>
          </table>
        </div>
      )}

      {chartRow && <AssetChartModal row={chartRow} onClose={() => setChartRow(null)} />}
      {/* Opens for ANY row with an ISIN — including the ~87% with no `company` row.
          The modal resolves those against GuruFocus and hands the listing back, which
          is what fills the Exchange / Ticker columns without reloading the coverage map. */}
      {dividendRow?.isin && (
        <AssetDividendModal row={dividendRow} isin={dividendRow.isin}
          entry={dividends[dividendRow.isin]}
          onClose={() => setDividendRow(null)}
          // Carries back both the resolved listing (fills Exchange/Ticker) and whether
          // it pays anything (flips the cell to View or NO PAYOUTS).
          onResolved={(e) => setDividends((d) => ({ ...d, [dividendRow.isin]: e }))} />
      )}
      {financialRow?.row.isin && (
        <AssetFinancialModal row={financialRow.row} isin={financialRow.row.isin} item={financialRow.item}
          onClose={() => setFinancialRow(null)}
          // Same coverage map as Div/share (one ISIN→listing bridge), so a fetch here
          // badges NO DATA there too without a reload. `has_financials` is about the
          // BLOB, so it's shared by both income-statement columns.
          onLoaded={(hasFinancials) => setDividends((d) => {
            const i = financialRow.row.isin;
            return i && d[i] ? { ...d, [i]: { ...d[i], has_financials: hasFinancials } } : d;
          })} />
      )}
      {resolveRow && (
        <RowResolveModal row={resolveRow}
          onClose={(didResolve) => { setResolveRow(null); if (didResolve) void load(); }} />
      )}
      {showCreate && (
        <CreateUniverseModal
          sectorOptions={[...new Set(((rows ?? []).map((r) => r.sector).filter(Boolean) as string[]))].sort()}
          universes={universes}
          onDeleted={() => { setUniverseFilter(''); void loadUniverses(); }}
          onClose={(created) => { setShowCreate(false); if (created) void loadUniverses(); }} />
      )}
    </section>
  );
}

// A cell that CAN'T be filled says WHY. An empty cell is a claim ("nothing here"), and
// for a dividend that claim reads as "pays nothing" — which is a different, and usually
// false, statement. Each badge below names the actual reason, and each is a dead end we
// have already paid for once and cached, so it does not invite a pointless retry.
const DIV_REASON: Record<string, { label: string; tone: string; title: string }> = {
  not_found: {
    label: 'NO LISTING', tone: 'bg-overlay/[0.06] text-fg-muted border-neutral-700',
    title: 'GuruFocus does not know this ISIN — it has no listing for it, so there is nothing to price. (Cached: we asked once.)',
  },
  unsubscribed: {
    label: 'UNSUBSCRIBED', tone: 'bg-warn-500/15 text-warn-300 border-warn-500/25',
    title: 'GuruFocus lists this ISIN only on exchanges outside our subscription, so no dividend data is obtainable for it.',
  },
  no_payouts: {
    label: 'NO PAYOUTS', tone: 'bg-overlay/[0.06] text-fg-muted border-neutral-700',
    title: 'Resolved, fetched, and GuruFocus returned no payments at all — this instrument distributes nothing. An ACCUMULATING ETF (e.g. iShares Core MSCI World) reinvests instead of paying out, and plenty of stocks (Berkshire) simply never declare a dividend. An empty history here is the ANSWER, not a gap.',
  },
  not_applicable: {
    label: 'NOT EQUITY', tone: 'bg-overlay/[0.06] text-fg-faint border-neutral-800',
    title: 'This ISIN is a bond, future or FX instrument — not an equity listing. It pays coupons or nothing at all, never a dividend per share, so the question does not apply. (No API call was spent: 30% of the grid is bonds.)',
  },
  no_data: {
    label: 'NO DATA', tone: 'bg-warn-500/10 text-warn-300/80 border-warn-500/20',
    title: 'The ISIN resolved to a real listing, but GuruFocus holds no record for it — typically a dead OTC line of an acquired or delisted company (e.g. Micro Focus → OTCPK:MCFUF after the OpenText takeover). This is a GAP, not a claim that the value is zero — which is exactly why it is not badged NO PAYOUTS.',
  },
  fund: {
    label: 'FUND', tone: 'bg-overlay/[0.06] text-fg-faint border-neutral-800',
    title: 'A fund has no revenue: it HOLDS securities, it does not operate a business. GuruFocus agrees — it returns no financials for an ETF at all. A category error, not a data gap, so no API call is spent asking.',
  },
};

// Products that structurally cannot have the thing a column measures. Answered from the
// grid row alone — no resolution, no API call. Together these are ~59% of the grid, and
// spending a GuruFocus call to be told "bonds have no revenue" is the kind of quota burn
// the dividend backfill already caught once.
const NON_EQUITY_PRODUCTS = new Set(['BONDS', 'FUTURE', 'FX', 'CRYPTO_CURRENCY']);
const FUND_PRODUCTS = new Set(['ETF', 'FUNDS']);

/** Revenue exists only for an operating business. Everything else is decided locally. */
function revenueReason(r: AssetGridRow): keyof typeof DIV_REASON | null {
  const product = (r.leonteq_product_type ?? '').toUpperCase();
  if (NON_EQUITY_PRODUCTS.has(product)) return 'not_applicable';
  if (FUND_PRODUCTS.has(product) || r.asset_class === 'etf') return 'fund';
  return null;
}

function ReasonBadge({ reason }: { reason: keyof typeof DIV_REASON }) {
  const r = DIV_REASON[reason];
  return (
    <span title={r.title}
      className={`text-[9px] uppercase tracking-wider font-semibold px-1 py-0.5 rounded border cursor-help ${r.tone}`}>
      {r.label}
    </span>
  );
}

/** The Div/share cell states — the SAME four for a stock and an ETF, because both are
 * the same thing underneath: a timeseries of (date, cash per unit held).
 *   never asked GuruFocus about this ISIN     -> "Fetch"        (resolves, then charts)
 *   asked; GuruFocus has no listing for it    -> NO LISTING     (badge, negative-cached)
 *   resolved, exchange outside our GF sub     -> UNSUBSCRIBED   (badge)
 *   resolved + fetched, pays nothing at all   -> NO PAYOUTS     (badge)
 *   otherwise                                 -> "View"
 *
 * Every dead end names ITSELF. "NO PAYOUTS" and "NO LISTING" look identical as a blank
 * cell and mean opposite things: one is an answer about the instrument (an accumulating
 * ETF, a non-paying stock), the other is a gap in our reach.
 *
 * A row resolved to a listing that is NOT its own (`is_home === false`) still charts:
 * the amounts are the same declaration-currency numbers (GuruFocus reports Apple's
 * 0.27 USD on Nasdaq, Xetra, Zurich and Milan alike). Only the HISTORY may be short,
 * which the modal says out loud. */
function DividendCell({ entry, onOpen }: { entry?: DividendCoverageEntry; onOpen: () => void }) {
  // Never resolved. Not "no dividend" — just "we have not asked yet".
  if (!entry) {
    return (
      <button type="button" onClick={onOpen}
        title="Resolve this ISIN to a GuruFocus listing and fetch its dividends (one API call, cached forever — including the misses)."
        className="text-[10px] px-2 py-0.5 rounded border border-neutral-700 text-fg-muted hover:text-accent-300 hover:border-accent-500/50 transition-colors">
        Fetch
      </button>
    );
  }
  if (entry.status && entry.status !== 'ok') {
    return <ReasonBadge reason={entry.status in DIV_REASON ? entry.status : 'not_found'} />;
  }
  if (entry.gf_unsubscribed) return <ReasonBadge reason="unsubscribed" />;
  // Fetched once, and it pays nothing — an accumulating fund, or a stock that has never
  // declared a dividend. Three-valued on purpose: `null` (never looked) still offers a
  // Fetch below, so "we haven't asked" can't masquerade as "there is nothing".
  if (entry.has_payments === false) return <ReasonBadge reason="no_payouts" />;
  // "View", not "Chart": the Actions column's price-chart button sits immediately
  // to the left and is already labelled Chart. Two adjacent "Chart" buttons that
  // open different things is a trap.
  return (
    <button type="button" onClick={onOpen}
      title="Cash paid per unit held — the payment history, in native currency and EUR. Fetched from GuruFocus on first open and cached."
      className="text-[10px] px-2 py-0.5 rounded border border-neutral-700 text-accent-400 transition-colors hover:border-accent-500/50">
      View
    </button>
  );
}

/** An income-statement cell (Revenue, Gross profit, …). Same bridge and badges as
 * Div/share, plus one dead end Div/share doesn't have: an ETF PAYS dividends but has no
 * income statement, so a fund is a category error here and an answer there. Decided from
 * the grid row before any resolution, so ~59% of rows cost nothing.
 *
 * `has_financials` is per-LISTING, not per-column — one blob carries every line — so both
 * columns share it. Whether a PARTICULAR line exists (a bank has no gross profit) is
 * computed from the blob and told in the modal, because it needs the blob to know. */
function FinancialCell({ r, entry, label, onOpen }: {
  r: AssetGridRow; entry?: DividendCoverageEntry; label: string; onOpen: () => void;
}) {
  const local = revenueReason(r);
  if (local) return <ReasonBadge reason={local} />;
  if (entry?.status && entry.status !== 'ok') {
    return <ReasonBadge reason={entry.status in DIV_REASON ? entry.status : 'not_found'} />;
  }
  if (entry?.gf_unsubscribed) return <ReasonBadge reason="unsubscribed" />;
  if (entry?.has_financials === false) return <ReasonBadge reason="no_data" />;
  return (
    <button type="button" onClick={onOpen}
      title={`${label} in millions — the listing's trading currency and EUR. Fetched from GuruFocus on first open and cached (one blob carries every line, and it's shared with the earnings pipeline, so the second column on the same company is free).`}
      className="text-[10px] px-2 py-0.5 rounded border border-neutral-700 text-accent-400 transition-colors hover:border-accent-500/50">
      View
    </button>
  );
}

function GridRow({ r, onChart, onResolve, dividend, onDividends, onFinancial, measureRef, dataIndex }: {
  r: AssetGridRow;
  onChart: () => void;
  onResolve: () => void;
  dividend?: DividendCoverageEntry;
  onDividends: () => void;
  onFinancial: (item: LineItem) => void;
  measureRef?: (el: HTMLTableRowElement | null) => void;
  dataIndex?: number;
}) {
  const hasSeries = r.analysis_id != null && (r.bars ?? 0) > 0;
  // A source is "missing" once the row was ATTEMPTED (not a fresh 'queued'
  // placeholder) but that source came back empty: no OpenFIGI FIGI, or no
  // priceable yfinance listing (status not 'ok').
  const attempted = r.status !== 'queued';
  const openfigiMissing = attempted && !r.openfigi_figi;
  const yfinanceMissing = attempted && r.status !== 'ok';
  // Issuer domiciled somewhere other than the venue it trades on — an ADR (TSM,
  // ASML), a GDR (SMSN.IL), or a pan-European MTF line (ATCOBS.XC). An ETF has no
  // domicile at all, so it never reads as cross-listed.
  const crossListed = !!r.domicile_country && !!r.listing_country
    && r.domicile_country !== r.listing_country;
  return (
      <tr ref={measureRef} data-index={dataIndex} className="hover:bg-accent-500/10 transition-colors">
        <td className="px-3 py-1.5 font-mono text-fg whitespace-nowrap">
          <span title={`${r.status}${r.reason ? ' — ' + r.reason : ''}`}
            className={`inline-block h-2 w-2 rounded-full align-middle mr-2 ${STATUS_DOT[r.status] ?? 'bg-neutral-500'}`} />
          {r.isin}
          {r.leonteq_verified && (
            <span title="In the uploaded Leonteq (lynqs) list"
              className="ml-2 align-middle text-[8px] uppercase tracking-wider font-semibold px-1 py-0.5 rounded border bg-accent-600/15 text-accent-400 border-accent-600/30">
              Leonteq ✓
            </span>
          )}
        </td>
        {/* Leonteq (lynqs) group — from the uploaded list (no separator: ISIN is Leonteq too) */}
        <td className="px-3 py-1.5 text-fg-soft">
          <span className="inline-block max-w-[180px] truncate align-bottom" title={r.leonteq_name ?? ''}>{r.leonteq_name ?? '—'}</span>
        </td>
        <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">
          <span className="inline-block max-w-[110px] truncate align-bottom" title={r.leonteq_product_type ?? ''}>{r.leonteq_product_type ?? '—'}</span>
        </td>
        <td className="px-3 py-1.5 font-mono text-fg-muted whitespace-nowrap">{r.leonteq_currency ?? '—'}</td>
        {/* OpenFIGI confirmation group */}
        <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap border-l border-neutral-800/40">
          {openfigiMissing
            ? <MissingBadge />
            : <span className="inline-block max-w-[200px] truncate align-bottom" title={r.openfigi_name ?? ''}>{r.openfigi_name ?? '—'}</span>}
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          {openfigiMissing ? <MissingBadge /> : <MatchBadge status={r.identity_status} />}
        </td>
        {/* yfinance group — when the source is missing, EVERY column in the group
            shows the missing badge (not just the anchor). */}
        <td className="px-3 py-1.5 text-fg-soft border-l border-neutral-800/40">
          {yfinanceMissing ? <MissingBadge />
            : <span className="inline-block max-w-[200px] truncate align-bottom" title={r.name ?? ''}>{r.name ?? '—'}</span>}
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
          ) : yfinanceMissing ? <MissingBadge /> : (r.yahoo_symbol ?? '—')}
        </td>
        <td className="px-3 py-1.5 font-mono text-fg-muted whitespace-nowrap">{yfinanceMissing ? <MissingBadge /> : (r.currency ?? '—')}</td>
        <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">{yfinanceMissing ? <MissingBadge /> : (classLabel(r.asset_class) || '—')}</td>
        <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">
          {yfinanceMissing ? <MissingBadge /> : (
            <span className="inline-flex items-center gap-1 max-w-[160px] align-bottom" title={r.sector ?? ''}>
              <span className="truncate">{sectorLabel(r.sector) || '—'}</span>
              {r.short_multiplier != null && (
                <span className="shrink-0 text-[9px] font-mono font-semibold px-1 py-0.5 rounded bg-neg-500/15 text-neg-300 border border-neg-500/20"
                  title={`Inverse ${r.short_multiplier}× leverage`}>−{r.short_multiplier}×</span>
              )}
            </span>
          )}
        </td>
        {/* Country — the issuer's domicile. When it lists somewhere else (an ADR,
            a GDR, a pan-European MTF line) the venue is shown as "via …", the
            same idiom the Symbol column uses for the tradable listing. */}
        <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">
          {yfinanceMissing ? <MissingBadge /> : (
            <span className="inline-flex items-baseline gap-1 max-w-[190px] align-bottom"
              title={crossListed ? `Domiciled in ${r.domicile_country} · lists in ${r.listing_country}` : (r.country ?? '')}>
              <span className="truncate">{r.country ?? '—'}</span>
              {crossListed && (
                <span className="shrink-0 text-[10px] text-fg-faint">via {r.listing_country}</span>
              )}
            </span>
          )}
        </td>
        <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">{yfinanceMissing ? <MissingBadge /> : (r.continent ?? '—')}</td>
        <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">
          {yfinanceMissing ? <MissingBadge />
            : r.msci_region == null
              ? <span className="text-fg-faint" title="This country has no MSCI market">—</span>
              : r.msci_region}
        </td>
        <td className="px-3 py-1.5 text-right font-mono text-fg whitespace-nowrap">{yfinanceMissing ? <MissingBadge /> : adv(r.med_adv_eur)}</td>
        <td className="px-3 py-1.5 text-right font-mono text-fg-soft whitespace-nowrap">{mcap(r.market_cap_eur)}</td>
        <td className="px-3 py-1.5 text-right font-mono text-fg-subtle whitespace-nowrap">{yfinanceMissing ? <MissingBadge /> : (r.price_from ?? '—')}</td>
        <td className="px-3 py-1.5 text-right font-mono text-fg-subtle whitespace-nowrap">{yfinanceMissing ? <MissingBadge /> : (r.price_to ?? '—')}</td>
        <td className="px-3 py-1.5 text-right font-mono text-fg-subtle whitespace-nowrap">{yfinanceMissing ? <MissingBadge /> : (r.volume_from ?? '—')}</td>
        <td className="px-3 py-1.5 text-right font-mono text-fg-subtle whitespace-nowrap">{yfinanceMissing ? <MissingBadge /> : (r.volume_to ?? '—')}</td>
        <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
          {yfinanceMissing ? <MissingBadge />
            : r.zero_vol_frac == null
              ? <span className="text-fg-faint">—</span>
              : <span className={r.zero_vol_frac >= 0.05 ? 'text-warn-300' : 'text-fg-subtle'}>{(r.zero_vol_frac * 100).toFixed(1)}%</span>}
        </td>
        {/* Actions — Chart (if priced) + manual OpenFIGI/yfinance resolve */}
        <td className="px-3 py-1.5 whitespace-nowrap border-l border-neutral-800/40">
          <div className="flex items-center gap-1.5">
            {hasSeries && (
              <button type="button" onClick={onChart} title="View native + EUR price & volume charts"
                className="text-[10px] px-2 py-0.5 rounded border border-neutral-700 text-accent-400 hover:border-accent-500/50 transition-colors">
                Chart
              </button>
            )}
            <button type="button" onClick={onResolve} title="Fetch OpenFIGI + yfinance for this row now"
              className="text-[10px] px-2 py-0.5 rounded border border-neutral-700 text-fg-muted hover:text-accent-300 hover:border-accent-500/50 transition-colors">
              Resolve
            </button>
          </div>
        </td>
        {/* GuruFocus group (bridged by ISIN) — the listing we query, then its
            dividends. An em-dash means this ISIN hasn't been resolved (hit Fetch) or
            couldn't be, exactly as in the Div/share cell beside it. */}
        <td className="px-3 py-1.5 font-mono text-fg-muted whitespace-nowrap border-l border-neutral-800/40">
          {dividend?.exchange ?? <span className="text-fg-faint">—</span>}
        </td>
        <td className="px-3 py-1.5 font-mono whitespace-nowrap">
          {dividend?.gurufocus_ticker
            ? (
              // A non-home listing is charted (same declaration-currency amounts) but
              // its payment HISTORY may be partial — GuruFocus holds 35 of Apple's 91
              // payments on Milan. Flag it here so it's visible without opening the
              // modal, which is where the full caveat lives.
              <span className={dividend.is_home === false ? 'text-warn-300 cursor-help' : 'text-fg'}
                title={dividend.is_home === false
                  ? `Not this row's own listing — GuruFocus has no ${r.currency ?? 'local'} line for this ISIN, so we use ${dividend.exchange}:${dividend.gurufocus_ticker}. The amounts are right (GuruFocus reports the declaration currency on every listing), but the payment history may be incomplete.`
                  : undefined}>
                {dividend.gurufocus_ticker}
                {dividend.is_home === false && <span className="ml-1 text-[9px]" aria-hidden>⚠</span>}
              </span>
            )
            : <span className="text-fg-faint">—</span>}
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <DividendCell entry={dividend} onOpen={onDividends} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Revenue" onOpen={() => onFinancial('revenue')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Gross profit" onOpen={() => onFinancial('gross_profit')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="EBIT" onOpen={() => onFinancial('ebit')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Interest expense" onOpen={() => onFinancial('interest_expense')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Pretax income" onOpen={() => onFinancial('pretax_income')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Income tax" onOpen={() => onFinancial('income_tax')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Net income" onOpen={() => onFinancial('net_income')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="D&A" onOpen={() => onFinancial('depreciation_amort')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Diluted EPS" onOpen={() => onFinancial('eps_diluted')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Operating CF" onOpen={() => onFinancial('operating_cash_flow')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Capex" onOpen={() => onFinancial('capex')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Total debt" onOpen={() => onFinancial('total_debt')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Cash & equivalents" onOpen={() => onFinancial('cash_and_equivalents')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Shareholders' equity" onOpen={() => onFinancial('shareholders_equity')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Shares outstanding" onOpen={() => onFinancial('shares_outstanding')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Forward EPS" onOpen={() => onFinancial('forward_eps')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Est. revenue growth" onOpen={() => onFinancial('revenue_growth_est')} />
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <FinancialCell r={r} entry={dividend} label="Est. long-term EPS growth" onOpen={() => onFinancial('eps_lt_growth_est')} />
        </td>
      </tr>
  );
}
