'use client';

import { useState, useEffect, useRef, useMemo } from 'react';
import type { Snapshot } from '../longequity/page';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type Company = {
  company_id: number;
  primary_ticker: string;
  primary_exchange: string;
  country: string | null;
  company_name: string | null;
  longequity_ticker: string | null;
};

type SnapshotData = {
  companies: Company[];
  added: Company[];
  removed: Company[];
};

type Region = 'USA' | 'EU' | 'Non-EU';

// Classify by PRIMARY EXCHANGE — where the company is listed determines data access.
const USA_EXCHANGES = new Set(['NYSE', 'NASDAQ', 'US', 'AMEX']);
const EU_EXCHANGES = new Set([
  'LSE', 'XTER', 'XPAR', 'XAMS', 'XBRU', 'MIL', 'XMAD', 'XSWX',
  'OSTO', 'STO', 'OCSE', 'OHEL', 'OSL', 'WAR', 'XPRA', 'GR',
  // legacy names
  'XETRA', 'EURONEXT', 'BME', 'BORSA', 'OMX', 'SIX', 'LN',
]);

function getRegion(exchange: string): Region {
  if (USA_EXCHANGES.has(exchange)) return 'USA';
  if (EU_EXCHANGES.has(exchange)) return 'EU';
  return 'Non-EU';
}

// Within each region, group companies by the country where their exchange is located.
const EXCHANGE_TO_LISTING_COUNTRY: Record<string, string> = {
  NYSE: 'United States', NASDAQ: 'United States', US: 'United States', AMEX: 'United States',
  LSE: 'United Kingdom', LN: 'United Kingdom',
  XTER: 'Germany', XETRA: 'Germany', GR: 'Germany',
  XPAR: 'France', EURONEXT: 'France',
  XAMS: 'Netherlands',
  XBRU: 'Belgium',
  MIL: 'Italy', BORSA: 'Italy',
  XMAD: 'Spain', BME: 'Spain',
  XSWX: 'Switzerland', SIX: 'Switzerland',
  OSTO: 'Sweden', STO: 'Sweden',
  OCSE: 'Denmark',
  OHEL: 'Finland', OMX: 'Finland',
  OSL: 'Norway',
  WAR: 'Poland',
  XPRA: 'Czech Republic',
  TSX: 'Canada', TSXV: 'Canada',
  ASX: 'Australia', NZSE: 'New Zealand',
  HKSE: 'Hong Kong', HKEX: 'Hong Kong',
  SSE: 'China', SZSE: 'China',
  XKRX: 'South Korea', KRX: 'South Korea',
  TWSE: 'Taiwan',
  TSE: 'Japan', JSE: 'Japan',
  NSE: 'India',
  BMV: 'Mexico',
  PM: 'Philippines',
  XS: 'International',
};

const EXCHANGE_TO_COUNTRY: Record<string, string> = {
  NYSE: 'United States', NASDAQ: 'United States', US: 'United States', AMEX: 'United States',
  LSE: 'United Kingdom', LN: 'United Kingdom',
  XTER: 'Germany', XETRA: 'Germany', GR: 'Germany',
  XPAR: 'France', EURONEXT: 'France',
  XAMS: 'Netherlands',
  XBRU: 'Belgium',
  MIL: 'Italy', BORSA: 'Italy',
  XMAD: 'Spain', BME: 'Spain',
  XSWX: 'Switzerland', SIX: 'Switzerland',
  OSTO: 'Sweden', STO: 'Sweden',
  OCSE: 'Denmark',
  OHEL: 'Finland', OMX: 'Finland',
  OSL: 'Norway',
  WAR: 'Poland',
  XPRA: 'Czech Republic',
  TSX: 'Canada', TSXV: 'Canada',
  ASX: 'Australia', NZSE: 'New Zealand',
  HKSE: 'Hong Kong', HKEX: 'Hong Kong',
  SSE: 'China', SZSE: 'China',
  XKRX: 'South Korea', KRX: 'South Korea',
  TWSE: 'Taiwan',
  TSE: 'Japan', JSE: 'Japan',
  NSE: 'India',
  BMV: 'Mexico',
  PM: 'Philippines',
  XS: 'International',
};

function getListingCountry(exchange: string): string {
  return EXCHANGE_TO_LISTING_COUNTRY[exchange] ?? exchange ?? 'Unknown';
}

// target_date is first day of the NEXT month after the data month
// e.g. "2025-09-01" → "August 2025"
function snapshotLabel(dateStr: string): string {
  const [year, month] = dateStr.split('-').map(Number);
  const d = new Date(year, month - 2);
  return d.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
}

function snapshotShortLabel(dateStr: string): string {
  const [year, month] = dateStr.split('-').map(Number);
  const d = new Date(year, month - 2);
  return d.toLocaleDateString('en-US', { month: 'short', year: '2-digit' });
}

const REGION_ORDER: Region[] = ['USA', 'EU', 'Non-EU'];

function CompanyRow({ c }: { c: Company }) {
  return (
    <div className="grid grid-cols-[6rem_6rem_5rem_7rem_1fr] gap-x-3 py-0.5 pl-4 text-xs font-mono hover:bg-gray-900 rounded">
      <span className="text-gray-400 truncate">{c.longequity_ticker ?? '—'}</span>
      <span className="text-white font-semibold truncate">{c.primary_ticker}</span>
      <span className="text-gray-500 truncate">{c.primary_exchange}</span>
      <span className="text-gray-500 truncate">{c.country?.trim() ?? '—'}</span>
      <span className="text-gray-400 truncate">{c.company_name ?? '—'}</span>
    </div>
  );
}

function CompanyTableHeader() {
  return (
    <div className="grid grid-cols-[6rem_6rem_5rem_7rem_1fr] gap-x-3 py-0.5 pl-4 text-xs font-mono text-gray-600 border-b border-gray-800 mb-0.5">
      <span>LE Ticker</span>
      <span>Primary</span>
      <span>Exchange</span>
      <span>Country</span>
      <span>Name</span>
    </div>
  );
}

function CountryGroup({
  country,
  companies,
}: {
  country: string;
  companies: Company[];
}) {
  const [open, setOpen] = useState(false);
  return (
    <div>
      <button
        onClick={() => setOpen((o) => !o)}
        className="w-full flex items-center gap-2 px-2 py-1 text-xs text-gray-300 hover:text-white hover:bg-gray-800 rounded transition-colors"
      >
        <span className="text-gray-500">{open ? '▾' : '▸'}</span>
        <span className="font-mono">{country}</span>
        <span className="ml-auto text-gray-500 font-mono">{companies.length}</span>
      </button>
      {open && (
        <div className="mb-1">
          <CompanyTableHeader />
          {companies.map((c) => (
            <CompanyRow key={c.company_id} c={c} />
          ))}
        </div>
      )}
    </div>
  );
}

function RegionSection({
  region,
  countryMap,
  total,
}: {
  region: Region;
  countryMap: Record<string, Company[]>;
  total: number;
}) {
  const [open, setOpen] = useState(true);
  const countries = Object.keys(countryMap).sort();

  return (
    <div className="border border-gray-800 rounded mb-3">
      <button
        onClick={() => setOpen((o) => !o)}
        className="w-full flex items-center gap-2 px-3 py-2 text-left hover:bg-gray-900 rounded transition-colors"
      >
        <span className="text-gray-400">{open ? '▾' : '▸'}</span>
        <span className="font-mono text-sm font-semibold text-white">{region}</span>
        <span className="text-xs text-gray-500 font-mono ml-auto">{total} companies</span>
      </button>
      {open && (
        <div className="border-t border-gray-800 px-2 py-1">
          {countries.map((country) => (
            <CountryGroup
              key={country}
              country={country}
              companies={countryMap[country]}
            />
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
        className={`flex items-center gap-2 text-xs font-mono px-2 py-1 rounded hover:bg-gray-800 transition-colors ${color}`}
      >
        <span>{open ? '▾' : '▸'}</span>
        <span>{label} ({companies.length})</span>
      </button>
      {open && (
        <div className="mt-1 border border-gray-800 rounded px-2 py-1">
          <CompanyTableHeader />
          {companies.map((c) => (
            <CompanyRow key={c.company_id} c={c} />
          ))}
        </div>
      )}
    </div>
  );
}

const MONTH_NAMES = [
  '', 'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
];

function IngestPanel({ onDone }: { onDone: () => void }) {
  const [latestAvailable, setLatestAvailable] = useState<string | null>(null);
  const [loadingAvailable, setLoadingAvailable] = useState(true);
  const [ingesting, setIngesting] = useState(false);
  const [log, setLog] = useState<{ type: string; message: string }[]>([]);
  const logEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    fetch(`${API_URL}/api/longequity/latest-available`)
      .then((r) => r.json())
      .then((d) => {
        if (d.available) {
          setLatestAvailable(`${MONTH_NAMES[d.month]} ${d.year}`);
        } else {
          setLatestAvailable('Not found');
        }
      })
      .catch(() => setLatestAvailable('Unknown'))
      .finally(() => setLoadingAvailable(false));
  }, []);

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [log]);

  async function runIngest() {
    setIngesting(true);
    setLog([]);
    try {
      const res = await fetch(`${API_URL}/api/ingest/long-equity`, { method: 'POST' });
      const reader = res.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split('\n\n');
        buffer = parts.pop() ?? '';
        for (const part of parts) {
          for (const line of part.split('\n')) {
            if (line.startsWith('data: ')) {
              try {
                const event = JSON.parse(line.slice(6));
                setLog((prev) => [...prev, event]);
                if (event.type === 'done' || event.type === 'error') {
                  setIngesting(false);
                  onDone();
                }
              } catch {}
            }
          }
        }
      }
    } catch (e) {
      setLog((prev) => [...prev, { type: 'error', message: String(e) }]);
    }
    setIngesting(false);
  }

  return (
    <div className="border border-gray-800 rounded px-4 py-3 mb-4 space-y-3">
      <div className="flex items-center justify-between">
        <div className="font-mono text-xs space-y-0.5">
          <div className="text-gray-500">
            Latest available month from Longequity:{' '}
            <span className="text-white font-semibold">
              {loadingAvailable ? 'Checking...' : latestAvailable}
            </span>
          </div>
          {!loadingAvailable && (
            <div className="text-gray-600">
              as of: {new Date().toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' })}
            </div>
          )}
        </div>
        <button
          onClick={runIngest}
          disabled={ingesting}
          className="px-3 py-1 rounded text-xs font-mono bg-gray-700 hover:bg-gray-600 disabled:opacity-50 text-white transition-colors"
        >
          {ingesting ? 'Running...' : 'Run ingest pipeline'}
        </button>
      </div>
      {log.length > 0 && (
        <div className="bg-gray-950 border border-gray-800 rounded p-2 max-h-64 overflow-y-auto font-mono text-xs space-y-0.5">
          {log.map((entry, i) => (
            <div
              key={i}
              className={
                entry.type === 'error'
                  ? 'text-red-400'
                  : entry.type === 'done'
                  ? 'text-green-400'
                  : 'text-gray-400'
              }
            >
              {entry.message || '\u00a0'}
            </div>
          ))}
          <div ref={logEndRef} />
        </div>
      )}
    </div>
  );
}

function escapeCsv(value: string | null | undefined): string {
  const s = value?.trim() ?? '';
  return s.includes(',') || s.includes('"') || s.includes('\n')
    ? `"${s.replace(/"/g, '""')}"`
    : s;
}

function downloadCsv(companies: Company[], targetDate: string): void {
  const header = ['longequity_ticker', 'primary_ticker', 'primary_exchange', 'country', 'name'];
  const rows = companies
    .slice()
    .sort((a, b) => (a.longequity_ticker ?? '').localeCompare(b.longequity_ticker ?? ''))
    .map((c) => [
      escapeCsv(c.longequity_ticker),
      escapeCsv(c.primary_ticker),
      escapeCsv(c.primary_exchange),
      escapeCsv(c.country),
      escapeCsv(c.company_name),
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

export default function LongEquityInsight({ snapshots }: { snapshots: Snapshot[] }) {
  const [selectedId, setSelectedId] = useState<number | null>(
    snapshots.length > 0 ? snapshots[snapshots.length - 1].snapshot_id : null,
  );
  const [data, setData] = useState<SnapshotData | null>(null);
  const [loading, setLoading] = useState(false);
  const [showIngest, setShowIngest] = useState(false);

  useEffect(() => {
    if (selectedId === null) return;
    setLoading(true);
    setData(null);
    fetch(`${API_URL}/api/longequity/companies/${selectedId}`)
      .then((r) => r.json())
      .then((d: SnapshotData) => {
        setData(d);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [selectedId]);

  const grouped = useMemo((): Record<Region, Record<string, Company[]>> | null => {
    if (!data) return null;
    const regions: Record<Region, Record<string, Company[]>> = {
      USA: {}, EU: {}, 'Non-EU': {},
    };
    for (const c of data.companies) {
      const region = getRegion(c.primary_exchange);
      const country = getListingCountry(c.primary_exchange);
      if (!regions[region][country]) regions[region][country] = [];
      regions[region][country].push(c);
    }
    for (const region of REGION_ORDER) {
      for (const country of Object.keys(regions[region])) {
        regions[region][country].sort((a, b) =>
          a.primary_ticker.localeCompare(b.primary_ticker),
        );
      }
    }
    return regions;
  }, [data]);

  if (snapshots.length === 0) {
    return (
      <div className="p-8 font-mono text-sm text-gray-500">
        No snapshots found. Run the ingest pipeline first.
      </div>
    );
  }

  const selectedSnapshot = snapshots.find((s) => s.snapshot_id === selectedId);

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-800 flex items-start justify-between">
        <div>
          <h1 className="font-mono text-base font-bold text-white">LongEquity Insight</h1>
          {selectedSnapshot && (
            <p className="text-xs text-gray-500 font-mono mt-0.5">
              {snapshotLabel(selectedSnapshot.target_date)}
            </p>
          )}
        </div>
        <button
          onClick={() => setShowIngest((v) => !v)}
          className="px-3 py-1 rounded text-xs font-mono text-gray-400 border border-gray-700 hover:text-white hover:border-gray-500 transition-colors"
        >
          {showIngest ? 'Hide ingest' : 'Ingest'}
        </button>
      </div>

      {/* Month tabs */}
      <div className="flex gap-1 px-6 py-2 border-b border-gray-800 overflow-x-auto">
        {snapshots.map((s) => (
          <button
            key={s.snapshot_id}
            onClick={() => setSelectedId(s.snapshot_id)}
            className={`shrink-0 px-3 py-1 rounded text-xs font-mono transition-colors ${
              s.snapshot_id === selectedId
                ? 'bg-gray-700 text-white'
                : 'text-gray-500 hover:text-white hover:bg-gray-800'
            }`}
          >
            {snapshotShortLabel(s.target_date)}
          </button>
        ))}
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto px-6 py-4">
        {showIngest && (
          <IngestPanel onDone={() => window.location.reload()} />
        )}

        {loading && (
          <p className="font-mono text-xs text-gray-500">Loading...</p>
        )}

        {!loading && data && (
          <>
            {/* CSV download */}
            <div className="mb-4 flex justify-end">
              <button
                onClick={() => downloadCsv(data.companies, selectedSnapshot?.target_date ?? 'export')}
                className="px-3 py-1 rounded text-xs font-mono text-gray-400 border border-gray-700 hover:text-white hover:border-gray-500 transition-colors"
              >
                Download CSV
              </button>
            </div>

            {/* Changes section */}
            {(data.added.length > 0 || data.removed.length > 0) && (
              <div className="mb-4 border border-gray-800 rounded px-3 py-2">
                <p className="font-mono text-xs text-gray-500 mb-2">Changes vs previous month</p>
                <ChangesBadge companies={data.added} label="Added" color="text-green-400" />
                <ChangesBadge companies={data.removed} label="Removed" color="text-red-400" />
              </div>
            )}

            {/* Region sections */}
            {grouped && REGION_ORDER.map((region) => {
              const countryMap = grouped[region];
              const total = Object.values(countryMap).reduce((s, arr) => s + arr.length, 0);
              if (total === 0) return null;
              return (
                <RegionSection
                  key={region}
                  region={region}
                  countryMap={countryMap}
                  total={total}
                />
              );
            })}
          </>
        )}
      </div>
    </div>
  );
}
