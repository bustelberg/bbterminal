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
    <div className="grid grid-cols-[6rem_6rem_5rem_7rem_1fr] gap-x-3 py-1.5 pl-4 text-sm hover:bg-white/[0.02] rounded-lg transition-colors">
      <span className="text-gray-500 truncate text-xs">{c.longequity_ticker ?? '—'}</span>
      <span className="text-white font-medium truncate">{c.primary_ticker}</span>
      <span className="text-gray-500 truncate text-xs">{c.primary_exchange}</span>
      <span className="text-gray-500 truncate text-xs">{c.country?.trim() ?? '—'}</span>
      <span className="text-gray-400 truncate">{c.company_name ?? '—'}</span>
    </div>
  );
}

function CompanyTableHeader() {
  return (
    <div className="grid grid-cols-[6rem_6rem_5rem_7rem_1fr] gap-x-3 py-1.5 pl-4 text-xs font-medium text-gray-500 border-b border-gray-800/40 mb-0.5">
      <span>LE Ticker</span>
      <span>Primary</span>
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
  const header = ['longequity_ticker', 'primary_ticker', 'primary_exchange', 'country', 'name'];
  const rows = companies
    .slice()
    .sort((a, b) => (a.longequity_ticker ?? '').localeCompare(b.longequity_ticker ?? ''))
    .map((c) => [
      escapeCsv(c.longequity_ticker), escapeCsv(c.primary_ticker),
      escapeCsv(c.primary_exchange), escapeCsv(c.country), escapeCsv(c.company_name),
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

export default function LongEquityInsight({ snapshots: initialSnapshots }: { snapshots: Snapshot[] }) {
  const [snapshots, setSnapshots] = useState<Snapshot[]>(initialSnapshots);
  const [selectedDate, setSelectedDate] = useState<string | null>(
    initialSnapshots.length > 0 ? initialSnapshots[initialSnapshots.length - 1].target_date : null,
  );
  const [data, setData] = useState<SnapshotData | null>(null);
  const [loading, setLoading] = useState(false);

  const [latestAvailable, setLatestAvailable] = useState<string | null>(null);
  const [loadingAvailable, setLoadingAvailable] = useState(true);
  const [ingesting, setIngesting] = useState(false);
  const [ingestLog, setIngestLog] = useState<{ type: string; message: string }[]>([]);
  const logEndRef = useRef<HTMLDivElement>(null);
  const didAutoIngest = useRef(false);

  async function refreshSnapshots() {
    try {
      const res = await fetch(`${API_URL}/api/longequity/snapshots`);
      if (res.ok) {
        const fresh: Snapshot[] = await res.json();
        setSnapshots(fresh);
        if (fresh.length > 0) setSelectedDate(fresh[fresh.length - 1].target_date);
      }
    } catch {}
  }

  useEffect(() => {
    if (didAutoIngest.current) return;
    fetch(`${API_URL}/api/longequity/latest-available`)
      .then((r) => r.json())
      .then((d) => {
        if (d.available) {
          setLatestAvailable(`${MONTH_NAMES[d.month]} ${d.year}`);
          const remoteKey = d.year * 100 + d.month;
          let shouldIngest = false;
          if (snapshots.length === 0) {
            shouldIngest = true;
          } else {
            const lastDate = snapshots[snapshots.length - 1].target_date;
            const [y, m] = lastDate.split('-').map(Number);
            if (remoteKey > y * 100 + m) shouldIngest = true;
          }
          if (shouldIngest) { didAutoIngest.current = true; runIngest(); }
        } else {
          setLatestAvailable('Not found');
        }
      })
      .catch(() => setLatestAvailable('Unknown'))
      .finally(() => setLoadingAvailable(false));
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [ingestLog]);

  const ingestingRef = useRef(false);

  async function runIngest() {
    if (ingestingRef.current) return;
    ingestingRef.current = true;
    setIngesting(true);
    setIngestLog([]);
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
                setIngestLog((prev) => [...prev, event]);
                if (event.type === 'done' || event.type === 'error') {
                  setIngesting(false);
                  refreshSnapshots();
                }
              } catch {}
            }
          }
        }
      }
    } catch (e) {
      setIngestLog((prev) => [...prev, { type: 'error', message: String(e) }]);
    }
    setIngesting(false);
    ingestingRef.current = false;
  }

  useEffect(() => {
    if (selectedDate === null) return;
    setLoading(true);
    setData(null);
    fetch(`${API_URL}/api/longequity/companies?target_date=${selectedDate}`)
      .then((r) => r.json())
      .then((d: SnapshotData) => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, [selectedDate]);

  const grouped = useMemo((): Record<Region, Record<string, Company[]>> | null => {
    if (!data) return null;
    const regions: Record<Region, Record<string, Company[]>> = { USA: {}, EU: {}, 'Non-EU': {} };
    for (const c of data.companies) {
      const region = getRegion(c.primary_exchange);
      const country = getListingCountry(c.primary_exchange);
      if (!regions[region][country]) regions[region][country] = [];
      regions[region][country].push(c);
    }
    for (const region of REGION_ORDER) {
      for (const country of Object.keys(regions[region])) {
        regions[region][country].sort((a, b) => a.primary_ticker.localeCompare(b.primary_ticker));
      }
    }
    return regions;
  }, [data]);

  if (snapshots.length === 0) {
    return (
      <div className="p-8">
        {ingestLog.length > 0 ? (
          <div className="bg-[#151821] border border-gray-800/40 rounded-xl p-4 max-h-96 overflow-y-auto text-sm space-y-0.5">
            {ingestLog.map((entry, i) => (
              <div key={i} className={
                entry.type === 'error' ? 'text-rose-400'
                : entry.type === 'done' ? 'text-emerald-400'
                : 'text-gray-400'
              }>
                {entry.message || '\u00a0'}
              </div>
            ))}
            <div ref={logEndRef} />
          </div>
        ) : (
          <p className="text-sm text-gray-500">
            {ingesting ? 'Starting ingest pipeline...' : 'No snapshots found.'}
          </p>
        )}
      </div>
    );
  }

  const selectedSnapshot = snapshots.find((s) => s.target_date === selectedDate);

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-8 py-5 border-b border-gray-800/60 flex items-center justify-between gap-6">
        <div>
          <h1 className="text-lg font-semibold text-white">LongEquity Insight</h1>
          {selectedSnapshot && (
            <p className="text-xs text-gray-500 mt-0.5">
              {snapshotLabel(selectedSnapshot.target_date)}
            </p>
          )}
        </div>
        <div className="flex items-center gap-4 shrink-0">
          <div className="text-right text-sm">
            <div className="text-gray-400">
              Latest available:{' '}
              <span className="text-white font-medium">
                {loadingAvailable ? 'Checking...' : latestAvailable}
              </span>
            </div>
            {!loadingAvailable && (
              <div className="text-xs text-gray-600">
                as of {new Date().toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' })}
              </div>
            )}
          </div>
          <button
            onClick={runIngest}
            disabled={ingesting}
            className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white transition-colors whitespace-nowrap"
          >
            {ingesting ? 'Running...' : 'Run ingest'}
          </button>
        </div>
      </div>

      {/* Month tabs */}
      <div className="flex gap-1.5 px-8 py-3 border-b border-gray-800/60 overflow-x-auto">
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

      {/* Content */}
      <div className="flex-1 overflow-auto px-8 py-5">
        {ingestLog.length > 0 && (
          <div className="bg-[#151821] border border-gray-800/40 rounded-xl p-4 max-h-52 overflow-y-auto text-sm space-y-0.5 mb-5">
            {ingestLog.map((entry, i) => (
              <div key={i} className={
                entry.type === 'error' ? 'text-rose-400'
                : entry.type === 'done' ? 'text-emerald-400'
                : 'text-gray-500'
              }>
                {entry.message}
              </div>
            ))}
            <div ref={logEndRef} />
          </div>
        )}

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
    </div>
  );
}
