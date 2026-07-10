'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import type { AssetGridRow, DividendCoverageEntry } from '../../lib/types/api';
import { classLabel, sectorLabel } from '../../lib/assetLabels';
import AssetChartModal from './AssetChartModal';
import AssetDividendModal from './AssetDividendModal';
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
  // ISIN. Div/share is the one column sourced from GuruFocus, reached by bridging
  // this row's ISIN to a GuruFocus company — so it earns its own badge.
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
                <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap border-l border-neutral-800/40"
                  title="Dividends per share (GuruFocus). Resolved from this row's ISIN to a GuruFocus company — most grid rows (ETFs, crypto, un-ingested equities) have none.">
                  <div className="flex flex-col gap-1 items-start">
                    <span>Div/share</span>
                    <SourceBadge source="GuruFocus" />
                  </div>
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {/* +2: the Div/share and Actions columns live outside COLS. */}
              {padTop > 0 && <tr aria-hidden><td colSpan={COLS.length + 2} style={{ height: padTop }} /></tr>}
              {vItems.map((vi) => {
                const r = view[vi.index];
                return (
                  <GridRow key={r.execution_id} r={r} onChart={() => setChartRow(r)}
                    onResolve={() => setResolveRow(r)}
                    dividend={r.isin ? dividends[r.isin] : undefined}
                    onDividends={() => setDividendRow(r)}
                    measureRef={rowVirtualizer.measureElement} dataIndex={vi.index} />
                );
              })}
              {padBottom > 0 && <tr aria-hidden><td colSpan={COLS.length + 2} style={{ height: padBottom }} /></tr>}
            </tbody>
          </table>
        </div>
      )}

      {chartRow && <AssetChartModal row={chartRow} onClose={() => setChartRow(null)} />}
      {dividendRow?.isin && dividends[dividendRow.isin] && (
        <AssetDividendModal row={dividendRow} companyId={dividends[dividendRow.isin].company_id}
          onClose={() => setDividendRow(null)}
          // Flip has_data locally so the cell stops offering a fetch.
          onFetched={() => setDividends((d) => {
            const isin = dividendRow.isin;
            return isin && d[isin] ? { ...d, [isin]: { ...d[isin], has_data: true } } : d;
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

/** The Div/share cell has four states, and conflating any two of them is a lie:
 *   no GuruFocus company behind this ISIN  -> "—" (majority of the grid)
 *   company found, exchange outside our GF subscription -> UNSUBSCRIBED
 *   company found, series already stored   -> "Chart"
 *   company found, nothing stored yet      -> "Fetch" (one lazy GF call)
 * A blank cell must never read as "this company pays no dividend". */
function DividendCell({ entry, onOpen }: { entry?: DividendCoverageEntry; onOpen: () => void }) {
  if (!entry) {
    return (
      <span className="text-fg-faint" title="No GuruFocus company is linked to this ISIN — ETFs, crypto, and equities we have never ingested have no dividend data.">
        —
      </span>
    );
  }
  if (entry.gf_unsubscribed) {
    return (
      <span className="text-[9px] uppercase tracking-wider px-1 py-0.5 rounded bg-warn-500/15 text-warn-300"
        title={`Exchange ${entry.exchange ?? '?'} is outside our GuruFocus subscription — no dividend data is obtainable for this listing.`}>
        UNSUBSCRIBED
      </span>
    );
  }
  // "View", not "Chart": the Actions column's price-chart button sits immediately
  // to the left and is already labelled Chart. Two adjacent "Chart" buttons that
  // open different things is a trap.
  return (
    <button type="button" onClick={onOpen}
      title={entry.has_data ? 'View the dividends-per-share history' : 'Fetch dividends from GuruFocus (one API call, cached)'}
      className={`text-[10px] px-2 py-0.5 rounded border border-neutral-700 transition-colors hover:border-accent-500/50 ${
        entry.has_data ? 'text-accent-400' : 'text-fg-muted hover:text-accent-300'}`}>
      {entry.has_data ? 'View' : 'Fetch'}
    </button>
  );
}

function GridRow({ r, onChart, onResolve, dividend, onDividends, measureRef, dataIndex }: {
  r: AssetGridRow;
  onChart: () => void;
  onResolve: () => void;
  dividend?: DividendCoverageEntry;
  onDividends: () => void;
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
        {/* Dividends per share (GuruFocus, bridged by ISIN) — the rightmost cell */}
        <td className="px-3 py-1.5 whitespace-nowrap border-l border-neutral-800/40">
          <DividendCell entry={dividend} onOpen={onDividends} />
        </td>
      </tr>
  );
}
