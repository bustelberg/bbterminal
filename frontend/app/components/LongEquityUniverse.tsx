'use client';

import { useState, useEffect, useMemo } from 'react';
import type { Snapshot } from '../longequity-universe/page';

import { ingestStore, startIngest } from '../../lib/stores/ingest';
import ProgressTimeline from './ProgressTimeline';
import { trackedFetch } from '../../lib/loading';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type Company = {
  company_id: number;
  gurufocus_ticker: string;
  gurufocus_exchange: string;
  country: string | null;
  company_name: string | null;
};

type SnapshotData = {
  companies: Company[];
  added: Company[];
  removed: Company[];
};

type Region = 'USA' | 'EU' | 'Non-EU';

const USA_EXCHANGES = new Set(['NYSE', 'NASDAQ', 'US', 'AMEX']);
const EU_EXCHANGES = new Set([
  'LSE', 'XTER', 'XPAR', 'XAMS', 'XBRU', 'MIL', 'XMAD', 'XSWX',
  'OSTO', 'STO', 'OCSE', 'OHEL', 'OSL', 'WAR', 'XPRA', 'GR',
  'XETRA', 'EURONEXT', 'BME', 'BORSA', 'OMX', 'SIX', 'LN',
]);

function getRegion(exchange: string): Region {
  if (USA_EXCHANGES.has(exchange)) return 'USA';
  if (EU_EXCHANGES.has(exchange)) return 'EU';
  return 'Non-EU';
}

const EXCHANGE_TO_LISTING_COUNTRY: Record<string, string> = {
  NYSE: 'United States', NASDAQ: 'United States', US: 'United States', AMEX: 'United States',
  LSE: 'United Kingdom', LN: 'United Kingdom',
  XTER: 'Germany', XETRA: 'Germany', GR: 'Germany',
  XPAR: 'France', EURONEXT: 'France',
  XAMS: 'Netherlands', XBRU: 'Belgium',
  MIL: 'Italy', BORSA: 'Italy',
  XMAD: 'Spain', BME: 'Spain',
  XSWX: 'Switzerland', SIX: 'Switzerland',
  OSTO: 'Sweden', STO: 'Sweden',
  OCSE: 'Denmark', OHEL: 'Finland', OMX: 'Finland',
  OSL: 'Norway', WAR: 'Poland', XPRA: 'Czech Republic',
  TSX: 'Canada', TSXV: 'Canada',
  ASX: 'Australia', NZSE: 'New Zealand',
  HKSE: 'Hong Kong', HKEX: 'Hong Kong',
  SSE: 'China', SZSE: 'China',
  XKRX: 'South Korea', KRX: 'South Korea',
  TWSE: 'Taiwan', TSE: 'Japan', JSE: 'Japan',
  NSE: 'India', BMV: 'Mexico', PM: 'Philippines',
  XS: 'International',
};

function getListingCountry(exchange: string): string {
  return EXCHANGE_TO_LISTING_COUNTRY[exchange] ?? exchange ?? 'Unknown';
}

function snapshotLabel(dateStr: string): string {
  const [year, month] = dateStr.split('-').map(Number);
  const d = new Date(year, month - 1);
  return d.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
}

function snapshotShortLabel(dateStr: string): string {
  const [year, month] = dateStr.split('-').map(Number);
  const d = new Date(year, month - 1);
  return d.toLocaleDateString('en-US', { month: 'short', year: '2-digit' });
}

const REGION_ORDER: Region[] = ['USA', 'EU', 'Non-EU'];

function CompanyRow({ c }: { c: Company }) {
  return (
    <div className="grid grid-cols-[6rem_5rem_7rem_1fr] gap-x-3 py-1.5 pl-4 text-sm hover:bg-white/[0.02] rounded-lg transition-colors">
      <span className="text-white font-medium truncate">{c.gurufocus_ticker}</span>
      <span className="text-gray-500 truncate text-xs">{c.gurufocus_exchange}</span>
      <span className="text-gray-500 truncate text-xs">{c.country?.trim() ?? '—'}</span>
      <span className="text-gray-400 truncate">{c.company_name ?? '—'}</span>
    </div>
  );
}

function CompanyTableHeader() {
  return (
    <div className="grid grid-cols-[6rem_5rem_7rem_1fr] gap-x-3 py-1.5 pl-4 text-xs font-medium text-gray-500 border-b border-gray-800/40 mb-0.5">
      <span>Ticker</span>
      <span>Exchange</span>
      <span>Country</span>
      <span>Name</span>
    </div>
  );
}

function CountryGroup({ country, companies }: { country: string; companies: Company[] }) {
  const [open, setOpen] = useState(false);
  return (
    <div>
      <button
        onClick={() => setOpen((o) => !o)}
        className="w-full flex items-center gap-2 px-3 py-2 text-sm text-gray-300 hover:text-white hover:bg-white/[0.03] rounded-lg transition-colors"
      >
        <span className="text-gray-500 text-xs">{open ? '\u25BE' : '\u25B8'}</span>
        <span>{country}</span>
        <span className="ml-auto text-gray-500 text-xs">{companies.length}</span>
      </button>
      {open && (
        <div className="mb-1">
          <CompanyTableHeader />
          {companies.map((c) => <CompanyRow key={c.company_id} c={c} />)}
        </div>
      )}
    </div>
  );
}

function RegionSection({ region, countryMap, total }: { region: Region; countryMap: Record<string, Company[]>; total: number }) {
  const [open, setOpen] = useState(true);
  const countries = Object.keys(countryMap).sort();

  return (
    <div className="bg-[#151821] border border-gray-800/40 rounded-xl mb-3 overflow-hidden">
      <button
        onClick={() => setOpen((o) => !o)}
        className="w-full flex items-center gap-3 px-5 py-3 text-left hover:bg-white/[0.02] transition-colors"
      >
        <span className="text-gray-400 text-xs">{open ? '\u25BE' : '\u25B8'}</span>
        <span className="text-sm font-semibold text-white">{region}</span>
        <span className="text-xs text-gray-500 ml-auto">{total} companies</span>
      </button>
      {open && (
        <div className="border-t border-gray-800/40 px-3 py-2">
          {countries.map((country) => (
            <CountryGroup key={country} country={country} companies={countryMap[country]} />
          ))}
        </div>
      )}
    </div>
  );
}

function ChangesBadge({ companies, label, color }: { companies: Company[]; label: string; color: string }) {
  const [open, setOpen] = useState(false);
  if (companies.length === 0) return null;
  return (
    <div className="mb-2">
      <button
        onClick={() => setOpen((o) => !o)}
        className={`flex items-center gap-2 text-sm font-medium px-3 py-1.5 rounded-lg hover:bg-white/[0.03] transition-colors ${color}`}
      >
        <span className="text-xs">{open ? '\u25BE' : '\u25B8'}</span>
        <span>{label} ({companies.length})</span>
      </button>
      {open && (
        <div className="mt-1 bg-[#151821] border border-gray-800/40 rounded-xl px-3 py-2 overflow-hidden">
          <CompanyTableHeader />
          {companies.map((c) => <CompanyRow key={c.company_id} c={c} />)}
        </div>
      )}
    </div>
  );
}

const MONTH_NAMES = [
  '', 'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
];

function escapeCsv(value: string | null | undefined): string {
  const s = value?.trim() ?? '';
  return s.includes(',') || s.includes('"') || s.includes('\n')
    ? `"${s.replace(/"/g, '""')}"`
    : s;
}

function downloadCsv(companies: Company[], targetDate: string): void {
  const header = ['gurufocus_ticker', 'gurufocus_exchange', 'country', 'name'];
  const rows = companies
    .slice()
    .sort((a, b) => a.gurufocus_ticker.localeCompare(b.gurufocus_ticker))
    .map((c) => [
      escapeCsv(c.gurufocus_ticker),
      escapeCsv(c.gurufocus_exchange), escapeCsv(c.country), escapeCsv(c.company_name),
    ].join(','));

  const csv = [header.join(','), ...rows].join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `longequity_${targetDate}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

export default function LongEquityUniverse() {
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [snapshotsLoading, setSnapshotsLoading] = useState(false);
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [data, setData] = useState<SnapshotData | null>(null);
  const [loading, setLoading] = useState(false);
  const [hasLoaded, setHasLoaded] = useState(false);

  const [latestAvailable, setLatestAvailable] = useState<string | null>(null);
  const [loadingAvailable, setLoadingAvailable] = useState(false);
  const ingesting = ingestStore.use((s) => s.running);
  const ingestLog = ingestStore.use((s) => s.log);
  const setIngestLog = (next: typeof ingestLog) => ingestStore.set({ log: next });


  async function refreshSnapshots() {
    try {
      const res = await trackedFetch('Refreshing LongEquity snapshots', `${API_URL}/api/longequity/snapshots`);
      if (res.ok) {
        const fresh: Snapshot[] = await res.json();
        setSnapshots(fresh);
        if (fresh.length > 0) setSelectedDate(fresh[fresh.length - 1].target_date);
      }
    } catch {}
  }

  async function loadAll() {
    setHasLoaded(true);
    setSnapshotsLoading(true);
    setLoadingAvailable(true);
    const snapshotsPromise = (async () => {
      try {
        const res = await trackedFetch('Loading LongEquity snapshots', `${API_URL}/api/longequity/snapshots`);
        if (!res.ok) return;
        const fresh: Snapshot[] = await res.json();
        setSnapshots(fresh);
        if (fresh.length > 0) setSelectedDate(fresh[fresh.length - 1].target_date);
      } catch {
        // backend offline — surface nothing; empty-state UI handles it
      } finally {
        setSnapshotsLoading(false);
      }
    })();
    const availablePromise = (async () => {
      try {
        const res = await trackedFetch(
          'Checking latest available month',
          `${API_URL}/api/longequity/latest-available`,
        );
        if (!res.ok) {
          // Backend reachable but errored. Common causes: LONGEQUITY_BASE_URL
          // env var unset, remote upstream temporarily down. Surface it so the
          // user doesn't stare at a blank dash.
          let detail = '';
          try { detail = (await res.json())?.detail ?? ''; } catch {}
          setLatestAvailable(detail ? `Error: ${detail.slice(0, 80)}` : `Error ${res.status}`);
          return;
        }
        const d: { available: boolean; year?: number; month?: number } = await res.json();
        if (d.available && d.year && d.month) {
          setLatestAvailable(`${MONTH_NAMES[d.month]} ${d.year}`);
        } else {
          setLatestAvailable('Not found');
        }
      } catch (e) {
        setLatestAvailable(e instanceof Error ? `Error: ${e.message}` : 'Unknown');
      } finally {
        setLoadingAvailable(false);
      }
    })();
    await Promise.all([snapshotsPromise, availablePromise]);
  }

  function runIngest() {
    setHasLoaded(true);
    return startIngest(() => refreshSnapshots());
  }

  useEffect(() => {
    if (selectedDate === null) return;
    setLoading(true);
    setData(null);
    trackedFetch(`Loading companies for ${selectedDate}`, `${API_URL}/api/longequity/companies?target_date=${selectedDate}`)
      .then((r) => r.json())
      .then((d: SnapshotData) => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, [selectedDate]);

  const grouped = useMemo((): Record<Region, Record<string, Company[]>> | null => {
    if (!data) return null;
    const regions: Record<Region, Record<string, Company[]>> = { USA: {}, EU: {}, 'Non-EU': {} };
    for (const c of data.companies) {
      const region = getRegion(c.gurufocus_exchange);
      const country = getListingCountry(c.gurufocus_exchange);
      if (!regions[region][country]) regions[region][country] = [];
      regions[region][country].push(c);
    }
    for (const region of REGION_ORDER) {
      for (const country of Object.keys(regions[region])) {
        regions[region][country].sort((a, b) => a.gurufocus_ticker.localeCompare(b.gurufocus_ticker));
      }
    }
    return regions;
  }, [data]);

  const selectedSnapshot = snapshots.find((s) => s.target_date === selectedDate);

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-8 py-5 border-b border-gray-800/60 flex items-center justify-between gap-6">
        <div>
          <h1 className="text-lg font-semibold text-white">LongEquity Universe</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            Per-month snapshots of LongEquity reports. The cumulative <span className="font-mono">LongEquity</span> universe (every company ever seen) is rebuilt automatically at the end of each ingest.
          </p>
        </div>
        <div className="flex items-center gap-4 shrink-0">
          {hasLoaded && (
            <div className="text-right text-sm">
              <div className="text-gray-400">
                Latest available:{' '}
                <span className="text-white font-medium">
                  {loadingAvailable ? 'Checking...' : latestAvailable ?? '—'}
                </span>
              </div>
              {!loadingAvailable && latestAvailable && (
                <div className="text-xs text-gray-600">
                  as of {new Date().toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' })}
                </div>
              )}
            </div>
          )}
          <button
            onClick={loadAll}
            disabled={snapshotsLoading || loadingAvailable}
            className="px-4 py-2 rounded-lg text-sm font-medium border border-gray-700/60 text-gray-300 hover:text-white hover:border-gray-600 disabled:opacity-50 transition-colors whitespace-nowrap"
          >
            {snapshotsLoading || loadingAvailable ? 'Loading...' : hasLoaded ? 'Refresh' : 'Load'}
          </button>
          <button
            onClick={runIngest}
            disabled={ingesting}
            className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white transition-colors whitespace-nowrap"
          >
            {ingesting ? 'Running...' : 'Run ingest'}
          </button>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto px-8 py-5 space-y-5">


        {/* Ingest progress (shared store) */}
        {ingestLog.length > 0 && (() => {
          const lastDone = [...ingestLog].reverse().find(e => e.type === 'done');
          const lastError = [...ingestLog].reverse().find(e => e.type === 'error');
          return (
            <ProgressTimeline
              steps={[]}
              log={ingestLog.filter(e => e.message).map(e => e.message)}
              doneSummary={!ingesting ? lastDone?.message ?? null : null}
              errorMessage={lastError?.message ?? null}
              running={ingesting}
              defaultLogOpen
              title={ingesting ? 'Ingest progress' : 'Ingest complete'}
              onDismiss={() => setIngestLog([])}
            />
          );
        })()}

        {!hasLoaded ? (
          <p className="text-sm text-gray-500">
            Click <span className="text-gray-300">Load</span> to fetch saved snapshots, or <span className="text-gray-300">Run ingest</span> to pull the latest LongEquity data.
          </p>
        ) : snapshotsLoading ? (
          <div>
            <div className="h-4 w-32 rounded bg-gray-800/60 animate-pulse mb-3" />
            <div className="flex gap-1.5 pb-3 overflow-hidden">
              {Array.from({ length: 8 }).map((_, i) => (
                <div key={i} className="h-7 w-14 rounded-lg bg-gray-800/40 animate-pulse shrink-0" />
              ))}
            </div>
          </div>
        ) : snapshots.length === 0 ? (
          <p className="text-sm text-gray-500">
            {ingesting ? 'Starting ingest pipeline...' : 'No snapshots found. Run ingest to populate LongEquity data.'}
          </p>
        ) : (
          <>
            {/* Snapshot stats section */}
            <div>
              <div className="flex items-baseline justify-between mb-3">
                <h2 className="text-sm font-semibold text-white">
                  Snapshot stats
                  {selectedSnapshot && (
                    <span className="ml-2 text-xs font-normal text-gray-500">
                      {snapshotLabel(selectedSnapshot.target_date)}
                    </span>
                  )}
                </h2>
              </div>

              {/* Month tabs */}
              <div className="flex gap-1.5 pb-3 overflow-x-auto">
                {snapshots.map((s) => (
                  <button
                    key={s.target_date}
                    onClick={() => setSelectedDate(s.target_date)}
                    className={`shrink-0 px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                      s.target_date === selectedDate
                        ? 'bg-indigo-600/15 text-indigo-400'
                        : 'text-gray-500 hover:text-white hover:bg-white/5'
                    }`}
                  >
                    {snapshotShortLabel(s.target_date)}
                  </button>
                ))}
              </div>

              {loading && <p className="text-sm text-gray-500">Loading...</p>}

              {!loading && data && (
                <>
                  <div className="mb-5 flex justify-end">
                    <button
                      onClick={() => downloadCsv(data.companies, selectedSnapshot?.target_date ?? 'export')}
                      className="px-4 py-2 rounded-lg text-sm font-medium text-gray-400 border border-gray-700/60 hover:text-white hover:border-gray-600 transition-colors"
                    >
                      Download CSV
                    </button>
                  </div>

                  {(data.added.length > 0 || data.removed.length > 0) && (
                    <div className="mb-5 bg-[#151821] border border-gray-800/40 rounded-xl px-5 py-3">
                      <p className="text-xs font-medium text-gray-500 mb-2">Changes vs previous month</p>
                      <ChangesBadge companies={data.added} label="Added" color="text-emerald-400" />
                      <ChangesBadge companies={data.removed} label="Removed" color="text-rose-400" />
                    </div>
                  )}

                  {grouped && REGION_ORDER.map((region) => {
                    const countryMap = grouped[region];
                    const total = Object.values(countryMap).reduce((s, arr) => s + arr.length, 0);
                    if (total === 0) return null;
                    return <RegionSection key={region} region={region} countryMap={countryMap} total={total} />;
                  })}
                </>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
