'use client';

import { useState, useEffect, useMemo, useCallback } from 'react';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type Holding = {
  Ticker: string;
  Name: string;
  Sector: string;
  'Asset Class': string;
  'Market Value': string;
  'Weight (%)': string;
  'Notional Value': string;
  Quantity: string;
  Price: string;
  Location: string;
  Exchange: string;
  Currency: string;
  'FX Rate': string;
  gurufocus_url: string | null;
};

type Detail = {
  standard: string | null;
  effective_date: string | null;
  loading?: boolean;
  error?: string;
};

type Announcement = {
  date: string;
  title: string;
  href: string;
  is_constituent_change: boolean;
  is_other_country_coded: boolean;
  detail?: Detail;
};

type NetAddition = {
  title: string;
  company_name: string;
  country: string;
  date: string;
  effective_date: string | null;
  href: string;
  matched: boolean;
  matched_ticker: string | null;
  matched_name: string | null;
  match_method: string;
};

export default function AcwiUniverse() {
  const [holdings, setHoldings] = useState<Holding[]>([]);
  const [count, setCount] = useState(0);
  const [asOf, setAsOf] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState('');
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortAsc, setSortAsc] = useState(true);

  const [announcements, setAnnouncements] = useState<Announcement[]>([]);
  const [annLoading, setAnnLoading] = useState(true);
  const [annError, setAnnError] = useState<string | null>(null);
  const [constituentOnly, setConstituentOnly] = useState(true);
  const [annSearch, setAnnSearch] = useState('');
  // Manual detail fetches (keyed by href)
  const [manualDetails, setManualDetails] = useState<Record<string, Detail>>({});
  // SSE fetch progress
  const [fetchProgress, setFetchProgress] = useState<{
    message: string;
    fetched: number;
    total: number;
    pct: number;
    errors: number;
  } | null>(null);
  const [fetching, setFetching] = useState(false);
  const [fetchSummary, setFetchSummary] = useState<{
    message: string;
    errors: number;
    errorList: { title: string; href: string; error: string }[];
  } | null>(null);
  // Net additions
  const [netAdditions, setNetAdditions] = useState<NetAddition[]>([]);
  const [netAdditionsLoading, setNetAdditionsLoading] = useState(false);
  const [netAdditionsStats, setNetAdditionsStats] = useState<{ total: number; matched: number } | null>(null);
  const [netAdditionsSearch, setNetAdditionsSearch] = useState('');

  const loadNetAdditions = useCallback(async () => {
    setNetAdditionsLoading(true);
    try {
      const res = await fetch(`${API_URL}/api/acwi/net-additions`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setNetAdditions(data.net_additions);
      setNetAdditionsStats({ total: data.total, matched: data.matched });
    } catch {
      // silently fail — net additions are non-critical
    }
    setNetAdditionsLoading(false);
  }, []);

  const loadAnnouncements = useCallback(async () => {
    try {
      const res = await fetch(`${API_URL}/api/acwi/announcements`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setAnnouncements(data.announcements);
      return data.announcements as Announcement[];
    } catch (e) {
      setAnnError(e instanceof Error ? e.message : 'Failed to load announcements');
      return [] as Announcement[];
    }
  }, []);

  useEffect(() => {
    (async () => {
      try {
        const res = await fetch(`${API_URL}/api/acwi/holdings`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        setHoldings(data.holdings);
        setCount(data.count);
        setAsOf(data.as_of || '');
      } catch (e) {
        setError(e instanceof Error ? e.message : 'Failed to load');
      }
      setLoading(false);
    })();
    (async () => {
      const anns = await loadAnnouncements();
      setAnnLoading(false);

      // Check if any constituent changes are missing details
      const needsFetch = anns.some(
        (a: Announcement) => a.is_constituent_change && a.href && !a.detail
      );
      if (needsFetch) {
        // Auto-trigger SSE fetch for uncached details
        setFetching(true);
        setFetchSummary(null);
        setFetchProgress({ message: 'Starting...', fetched: 0, total: 0, pct: 0, errors: 0 });
        const es = new EventSource(`${API_URL}/api/acwi/fetch-all-details`);
        es.onmessage = (event) => {
          const data = JSON.parse(event.data);
          if (data.type === 'progress') {
            setFetchProgress({
              message: data.message,
              fetched: data.fetched ?? 0,
              total: data.total ?? 0,
              pct: data.pct ?? 0,
              errors: data.errors ?? 0,
            });
          } else if (data.type === 'done') {
            setFetchProgress(null);
            setFetching(false);
            setFetchSummary({
              message: data.message,
              errors: data.errors ?? 0,
              errorList: data.error_list ?? [],
            });
            es.close();
            loadAnnouncements();
            loadNetAdditions();
          } else if (data.type === 'error') {
            setFetchProgress(null);
            setFetching(false);
            setFetchSummary({
              message: `Error: ${data.message}`,
              errors: 1,
              errorList: [],
            });
            es.close();
          }
        };
        es.onerror = () => {
          setFetchProgress(null);
          setFetching(false);
          setFetchSummary({ message: 'Connection lost — partial results may have been cached', errors: -1, errorList: [] });
          es.close();
          loadAnnouncements();
          loadNetAdditions();
        };
      } else {
        // All details already cached — load net additions immediately
        loadNetAdditions();
      }
    })();
  }, [loadAnnouncements, loadNetAdditions]);

  const fetchDetail = useCallback(async (href: string) => {
    setManualDetails(prev => ({ ...prev, [href]: { standard: null, effective_date: null, loading: true } }));
    try {
      const res = await fetch(`${API_URL}/api/acwi/announcement-detail?url=${encodeURIComponent(href)}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setManualDetails(prev => ({ ...prev, [href]: { ...data, loading: false } }));
    } catch (e) {
      setManualDetails(prev => ({ ...prev, [href]: { standard: null, effective_date: null, loading: false, error: e instanceof Error ? e.message : 'Failed' } }));
    }
  }, []);

  // Get detail for an announcement: from inline (server-cached) or manual fetch
  const getDetail = useCallback((a: Announcement): Detail | undefined => {
    return a.detail || manualDetails[a.href];
  }, [manualDetails]);

  const filtered = useMemo(() => {
    let result = holdings;
    if (filter) {
      const q = filter.toLowerCase();
      result = result.filter(
        h =>
          h.Ticker.toLowerCase().includes(q) ||
          h.Name.toLowerCase().includes(q) ||
          h.Sector.toLowerCase().includes(q) ||
          h.Location.toLowerCase().includes(q)
      );
    }
    if (sortCol) {
      const numericCols = new Set(['Weight (%)', 'Market Value', 'Price', 'Quantity', 'FX Rate']);
      const isNumeric = numericCols.has(sortCol);
      result = [...result].sort((a, b) => {
        const av = (a as Record<string, string>)[sortCol] ?? '';
        const bv = (b as Record<string, string>)[sortCol] ?? '';
        if (isNumeric) {
          const na = parseFloat(av) || 0;
          const nb = parseFloat(bv) || 0;
          return sortAsc ? na - nb : nb - na;
        }
        return sortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
      });
    }
    return result;
  }, [holdings, filter, sortCol, sortAsc]);

  const sectorBreakdown = useMemo(() => {
    const map: Record<string, { count: number; weight: number }> = {};
    for (const h of holdings) {
      const s = h.Sector || 'Unknown';
      if (!map[s]) map[s] = { count: 0, weight: 0 };
      map[s].count++;
      map[s].weight += parseFloat(h['Weight (%)']) || 0;
    }
    return Object.entries(map)
      .sort((a, b) => b[1].weight - a[1].weight);
  }, [holdings]);

  const countryBreakdown = useMemo(() => {
    const map: Record<string, { count: number; weight: number }> = {};
    for (const h of holdings) {
      const c = h.Location || 'Unknown';
      if (!map[c]) map[c] = { count: 0, weight: 0 };
      map[c].count++;
      map[c].weight += parseFloat(h['Weight (%)']) || 0;
    }
    return Object.entries(map)
      .sort((a, b) => b[1].weight - a[1].weight);
  }, [holdings]);

  const filteredAnnouncements = useMemo(() => {
    let list = announcements;
    if (constituentOnly) list = list.filter(a => a.is_constituent_change);
    if (annSearch) {
      const q = annSearch.toLowerCase();
      list = list.filter(a => a.title.toLowerCase().includes(q));
    }
    return list;
  }, [announcements, constituentOnly, annSearch]);

  // Summary of details grouped by action
  const detailSummary = useMemo(() => {
    const groups: Record<string, { announcement: Announcement; detail: Detail }[]> = {
      ADDED: [],
      DELETED: [],
      'ADDED+DELETED': [],
      '-': [],
      'N/A': [],
    };
    for (const a of announcements) {
      if (!a.is_constituent_change) continue;
      const d = getDetail(a);
      if (!d || d.loading) continue;
      const action = d.standard || 'N/A';
      const key = action in groups ? action : 'N/A';
      groups[key].push({ announcement: a, detail: d });
    }
    return groups;
  }, [announcements, manualDetails, getDetail]);

  const otherCountryCoded = useMemo(() => {
    return announcements.filter(a => a.is_other_country_coded);
  }, [announcements]);

  const hasFetchedDetails = useMemo(() => {
    return announcements.some(a => a.detail) || Object.values(manualDetails).some(d => !d.loading);
  }, [announcements, manualDetails]);

  const fmtNum = (v: string) => {
    const n = parseFloat(v);
    if (isNaN(n)) return v;
    return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };

  const fmtMv = (v: string) => {
    const n = parseFloat(v);
    if (isNaN(n)) return v;
    if (n >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
    if (n >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
    return `$${n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  const actionStyle = (action: string) => {
    switch (action) {
      case 'ADDED': return 'bg-emerald-500/15 text-emerald-400';
      case 'DELETED': return 'bg-rose-500/15 text-rose-400';
      case 'ADDED+DELETED': return 'bg-amber-500/15 text-amber-400';
      default: return 'bg-gray-500/15 text-gray-400';
    }
  };

  return (
    <div className="p-8 space-y-6 max-w-[1400px] mx-auto">
      <div>
        <h1 className="text-2xl font-semibold text-white">MSCI ACWI Universe</h1>
        <p className="text-gray-400 text-sm mt-1">
          iShares MSCI ACWI ETF holdings &mdash; {count.toLocaleString()} equities
          {asOf && <> &mdash; as of {asOf}</>}
        </p>
      </div>

      {error && (
        <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-rose-400 text-sm">
          {error}
        </div>
      )}

      {/* Fetch progress bar */}
      {fetchProgress && (
        <div className="bg-indigo-500/10 border border-indigo-500/20 rounded-lg px-4 py-3 text-sm space-y-2">
          <div className="flex items-center gap-3 text-indigo-400">
            <span className="animate-pulse">●</span>
            <span>Fetching announcement details: {fetchProgress.message}</span>
          </div>
          {fetchProgress.total > 0 && (
            <div className="flex items-center gap-3">
              <div className="flex-1 h-1.5 rounded-full bg-gray-800 overflow-hidden">
                <div
                  className="h-full rounded-full bg-indigo-500 transition-all duration-300"
                  style={{ width: `${fetchProgress.pct}%` }}
                />
              </div>
              <span className="text-gray-400 font-mono text-xs w-12 text-right">{fetchProgress.pct}%</span>
            </div>
          )}
          {fetchProgress.errors > 0 && (
            <div className="text-rose-400 text-xs">{fetchProgress.errors} error{fetchProgress.errors !== 1 ? 's' : ''} so far</div>
          )}
        </div>
      )}

      {/* Fetch summary (persists after completion) */}
      {fetchSummary && !fetching && (
        <div className={`${fetchSummary.errors > 0 ? 'bg-amber-500/10 border-amber-500/20' : 'bg-emerald-500/10 border-emerald-500/20'} border rounded-lg px-4 py-3 text-sm space-y-2`}>
          <div className="flex items-center justify-between">
            <span className={fetchSummary.errors > 0 ? 'text-amber-400' : 'text-emerald-400'}>
              {fetchSummary.errors > 0 ? '⚠' : '✓'} {fetchSummary.message}
            </span>
            <button
              onClick={() => setFetchSummary(null)}
              className="text-gray-500 hover:text-gray-300 text-xs"
            >
              dismiss
            </button>
          </div>
          {fetchSummary.errorList.length > 0 && (
            <details className="text-xs">
              <summary className="text-rose-400 cursor-pointer hover:text-rose-300">
                Show {fetchSummary.errorList.length} failed announcement{fetchSummary.errorList.length !== 1 ? 's' : ''}
              </summary>
              <div className="mt-2 space-y-1 max-h-40 overflow-y-auto">
                {fetchSummary.errorList.map((e, i) => (
                  <div key={i} className="flex gap-2 text-gray-400">
                    <span className="text-rose-400 shrink-0">✗</span>
                    <span className="truncate">{e.title}</span>
                    <span className="text-gray-600 shrink-0">— {e.error}</span>
                  </div>
                ))}
              </div>
            </details>
          )}
        </div>
      )}

      {loading ? (
        <div className="text-gray-400 text-sm">Loading holdings...</div>
      ) : (
        <>
          {/* Summary cards */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Sector breakdown */}
            <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
              <h2 className="text-sm font-medium text-gray-300 mb-3">Sector Breakdown</h2>
              <div className="space-y-1.5">
                {sectorBreakdown.map(([sector, { count: c, weight }]) => (
                  <div key={sector} className="flex items-center gap-3 text-sm">
                    <div className="flex-1 text-gray-200 truncate">{sector}</div>
                    <div className="text-gray-400 font-mono text-xs w-12 text-right">{c}</div>
                    <div className="w-24">
                      <div className="h-1.5 rounded-full bg-gray-800 overflow-hidden">
                        <div
                          className="h-full rounded-full bg-indigo-500"
                          style={{ width: `${Math.min(weight * 3, 100)}%` }}
                        />
                      </div>
                    </div>
                    <div className="text-gray-300 font-mono text-xs w-16 text-right">{weight.toFixed(2)}%</div>
                  </div>
                ))}
              </div>
            </div>

            {/* Country breakdown (top 15) */}
            <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
              <h2 className="text-sm font-medium text-gray-300 mb-3">
                Top Countries <span className="text-gray-500 font-normal">({countryBreakdown.length} total)</span>
              </h2>
              <div className="space-y-1.5">
                {countryBreakdown.slice(0, 15).map(([country, { count: c, weight }]) => (
                  <div key={country} className="flex items-center gap-3 text-sm">
                    <div className="flex-1 text-gray-200 truncate">{country}</div>
                    <div className="text-gray-400 font-mono text-xs w-12 text-right">{c}</div>
                    <div className="w-24">
                      <div className="h-1.5 rounded-full bg-gray-800 overflow-hidden">
                        <div
                          className="h-full rounded-full bg-indigo-500"
                          style={{ width: `${Math.min(weight * 1.5, 100)}%` }}
                        />
                      </div>
                    </div>
                    <div className="text-gray-300 font-mono text-xs w-16 text-right">{weight.toFixed(2)}%</div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Constituent Changes Summary */}
          {hasFetchedDetails && (
            <div className="bg-[#151821] rounded-xl border border-gray-800/40">
              <div className="px-5 py-4 border-b border-gray-800/40">
                <h2 className="text-sm font-medium text-gray-300">
                  Constituent Changes Summary
                  {fetching && <span className="text-gray-500 font-normal ml-2 animate-pulse">fetching...</span>}
                </h2>
                <p className="text-gray-500 text-xs mt-0.5">Parsed from MSCI announcement details</p>
              </div>
              <div className="p-5 space-y-4">
                {(['ADDED', 'DELETED', 'ADDED+DELETED'] as const).map(action => {
                  const items = detailSummary[action];
                  if (!items || items.length === 0) return null;
                  return (
                    <div key={action}>
                      <div className="flex items-center gap-2 mb-2">
                        <span className={`text-xs font-medium px-2 py-0.5 rounded ${actionStyle(action)}`}>
                          {action}
                        </span>
                        <span className="text-gray-500 text-xs">{items.length} announcement{items.length !== 1 ? 's' : ''}</span>
                      </div>
                      <div className="overflow-x-auto max-h-[250px] overflow-y-auto">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-[#151821]">
                            <tr className="text-gray-400 text-xs uppercase tracking-wider">
                              <th className="text-left px-3 py-1.5 font-medium w-32">Date</th>
                              <th className="text-left px-3 py-1.5 font-medium">Announcement</th>
                              <th className="text-left px-3 py-1.5 font-medium w-40">Effective Date</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-gray-800/30">
                            {items.map(({ announcement: a, detail: d }, i) => (
                              <tr key={i} className="hover:bg-white/[0.02]">
                                <td className="px-3 py-2 text-gray-400 font-mono text-xs whitespace-nowrap">{a.date}</td>
                                <td className="px-3 py-2">
                                  <a
                                    href={a.href}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="text-indigo-400 hover:text-indigo-300 transition-colors"
                                  >
                                    {a.title}
                                  </a>
                                </td>
                                <td className="px-3 py-2 text-gray-300 font-mono text-xs whitespace-nowrap">
                                  {d.effective_date ?? ''}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Net Additions — added but never subsequently deleted */}
          {(netAdditions.length > 0 || netAdditionsLoading) && (
            <div className="bg-[#151821] rounded-xl border border-gray-800/40">
              <div className="px-5 py-4 border-b border-gray-800/40 flex items-center gap-4">
                <div className="flex-1">
                  <h2 className="text-sm font-medium text-gray-300">
                    Net Additions
                    {netAdditionsStats && (
                      <span className="text-gray-500 font-normal ml-2">
                        ({netAdditionsStats.total} total, {netAdditionsStats.matched} matched)
                      </span>
                    )}
                    {(netAdditionsLoading || fetching) && <span className="text-gray-500 font-normal ml-2 animate-pulse">loading...</span>}
                  </h2>
                  <p className="text-gray-500 text-xs mt-0.5">
                    Companies added to MSCI Standard Index and not subsequently deleted, matched against current ACWI holdings
                  </p>
                </div>
                <input
                  type="text"
                  placeholder="Search..."
                  value={netAdditionsSearch}
                  onChange={e => setNetAdditionsSearch(e.target.value)}
                  className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-1.5 text-sm text-gray-200 placeholder-gray-500 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none w-56"
                />
              </div>
              {netAdditions.length > 0 && (() => {
                const q = netAdditionsSearch.toLowerCase();
                const filtered = q
                  ? netAdditions.filter(item =>
                      item.company_name.toLowerCase().includes(q) ||
                      item.country.toLowerCase().includes(q) ||
                      (item.matched_ticker ?? '').toLowerCase().includes(q) ||
                      (item.matched_name ?? '').toLowerCase().includes(q)
                    )
                  : netAdditions;
                return (
                <div className="overflow-x-auto max-h-[500px] overflow-y-auto">
                  <table className="w-full text-sm">
                    <thead className="sticky top-0 bg-[#151821] z-10">
                      <tr className="text-gray-400 text-xs uppercase tracking-wider">
                        <th className="text-left px-3 py-1.5 font-medium w-10">#</th>
                        <th className="text-left px-3 py-1.5 font-medium w-14">CC</th>
                        <th className="text-left px-3 py-1.5 font-medium">Announcement</th>
                        <th className="text-left px-3 py-1.5 font-medium w-28">Added</th>
                        <th className="text-left px-3 py-1.5 font-medium w-16 text-center">Match</th>
                        <th className="text-left px-3 py-1.5 font-medium w-20">Ticker</th>
                        <th className="text-left px-3 py-1.5 font-medium">Matched Holding</th>
                        <th className="text-left px-3 py-1.5 font-medium w-20">Method</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-800/30">
                      {filtered.map((item, i) => (
                        <tr key={item.href} className="hover:bg-white/[0.02]">
                          <td className="px-3 py-2 text-gray-500 font-mono text-xs">{i + 1}</td>
                          <td className="px-3 py-2 text-gray-400 font-mono text-xs">{item.country}</td>
                          <td className="px-3 py-2 whitespace-nowrap">
                            <a
                              href={item.href}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="text-indigo-400 hover:text-indigo-300 transition-colors"
                            >
                              {item.company_name}
                            </a>
                          </td>
                          <td className="px-3 py-2 text-gray-400 font-mono text-xs whitespace-nowrap">{item.date}</td>
                          <td className="px-3 py-2 text-center">
                            {item.matched ? (
                              <span className="text-emerald-400">&#10003;</span>
                            ) : (
                              <span className="text-rose-400">&#10007;</span>
                            )}
                          </td>
                          <td className="px-3 py-2 text-gray-200 font-mono text-xs">{item.matched_ticker ?? ''}</td>
                          <td className="px-3 py-2 text-gray-300 text-xs whitespace-nowrap">
                            {item.matched_name ?? ''}
                          </td>
                          <td className="px-3 py-2 text-gray-500 font-mono text-xs">{item.match_method}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                );
              })()}
            </div>
          )}

          {/* MSCI Announcements */}
          <div className="bg-[#151821] rounded-xl border border-gray-800/40">
            <div className="px-5 py-4 border-b border-gray-800/40 flex items-center gap-4">
              <div>
                <h2 className="text-sm font-medium text-gray-300">
                  MSCI Index Announcements
                  {filteredAnnouncements.length > 0 && (
                    <span className="text-gray-500 font-normal ml-2">
                      ({filteredAnnouncements.length}{constituentOnly ? ` of ${announcements.length}` : ''})
                    </span>
                  )}
                </h2>
                <p className="text-gray-500 text-xs mt-0.5">
                  {constituentOnly ? 'Constituent changes only' : 'All announcements'} from MSCI Standard Indexes
                </p>
              </div>
              <input
                type="text"
                placeholder="Search announcements..."
                value={annSearch}
                onChange={e => setAnnSearch(e.target.value)}
                className="ml-auto bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-1.5 text-sm text-gray-200 placeholder-gray-500 w-64 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
              <button
                onClick={() => setConstituentOnly(v => !v)}
                className={`text-xs px-3 py-1.5 rounded-lg border transition-colors ${
                  constituentOnly
                    ? 'bg-indigo-600/20 border-indigo-500/40 text-indigo-400'
                    : 'bg-transparent border-gray-700 text-gray-400 hover:bg-white/5'
                }`}
              >
                {constituentOnly ? 'Constituent changes' : 'All announcements'}
              </button>
            </div>
            {annError && (
              <div className="mx-5 mt-3 bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-rose-400 text-sm">
                {annError}
              </div>
            )}
            {annLoading ? (
              <div className="px-5 py-4 text-gray-400 text-sm">Loading announcements...</div>
            ) : (
              <div className="overflow-x-auto max-h-[400px] overflow-y-auto">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-[#151821]">
                    <tr className="text-gray-400 text-xs uppercase tracking-wider">
                      <th className="text-left px-3 py-2.5 font-medium w-32">Date</th>
                      <th className="text-left px-3 py-2.5 font-medium">Announcement</th>
                      <th className="text-center px-3 py-2.5 font-medium w-20">Detail</th>
                      <th className="text-left px-3 py-2.5 font-medium w-24">Action</th>
                      <th className="text-left px-3 py-2.5 font-medium w-40">Effective Date</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-800/30">
                    {filteredAnnouncements.map((a, i) => {
                      const detail = getDetail(a);
                      return (
                        <tr key={i} className="hover:bg-white/[0.02]">
                          <td className="px-3 py-2.5 text-gray-400 font-mono text-xs whitespace-nowrap">{a.date}</td>
                          <td className="px-3 py-2.5">
                            {a.href ? (
                              <a
                                href={a.href}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-indigo-400 hover:text-indigo-300 transition-colors"
                              >
                                {a.title}
                              </a>
                            ) : (
                              <span className="text-gray-200">{a.title}</span>
                            )}
                          </td>
                          <td className="px-3 py-2.5 text-center">
                            {!detail && a.href && !fetching && (
                              <button
                                onClick={() => fetchDetail(a.href)}
                                className="text-xs px-2 py-1 rounded border border-gray-700 text-gray-400 hover:bg-white/5 hover:text-gray-200 transition-colors"
                              >
                                Fetch
                              </button>
                            )}
                            {!detail && fetching && (
                              <span className="text-gray-600 text-xs">...</span>
                            )}
                            {detail?.loading && (
                              <span className="text-gray-500 text-xs animate-pulse">...</span>
                            )}
                            {detail?.error && (
                              <span className="text-rose-400 text-xs" title={detail.error}>err</span>
                            )}
                          </td>
                          <td className="px-3 py-2.5">
                            {detail && !detail.loading && (
                              detail.standard ? (
                                <span className={`text-xs font-medium px-2 py-0.5 rounded ${actionStyle(detail.standard)}`}>
                                  {detail.standard}
                                </span>
                              ) : (
                                <span className="text-gray-600 text-xs">N/A</span>
                              )
                            )}
                          </td>
                          <td className="px-3 py-2.5 text-gray-300 font-mono text-xs whitespace-nowrap">
                            {detail && !detail.loading && (detail.standard ? detail.effective_date ?? '' : '')}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* Other country-coded announcements */}
          {otherCountryCoded.length > 0 && (
            <div className="bg-[#151821] rounded-xl border border-gray-800/40">
              <div className="px-5 py-4 border-b border-gray-800/40">
                <h2 className="text-sm font-medium text-gray-300">
                  Other Country-Coded Announcements
                  <span className="text-gray-500 font-normal ml-2">({otherCountryCoded.length})</span>
                </h2>
                <p className="text-gray-500 text-xs mt-0.5">Non-constituent announcements (updates, reviews, policy changes)</p>
              </div>
              <div className="overflow-x-auto max-h-[250px] overflow-y-auto">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-[#151821]">
                    <tr className="text-gray-400 text-xs uppercase tracking-wider">
                      <th className="text-left px-3 py-2.5 font-medium w-32">Date</th>
                      <th className="text-left px-3 py-2.5 font-medium">Announcement</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-800/30">
                    {otherCountryCoded.map((a, i) => (
                      <tr key={i} className="hover:bg-white/[0.02]">
                        <td className="px-3 py-2.5 text-gray-400 font-mono text-xs whitespace-nowrap">{a.date}</td>
                        <td className="px-3 py-2.5">
                          {a.href ? (
                            <a
                              href={a.href}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="text-indigo-400 hover:text-indigo-300 transition-colors"
                            >
                              {a.title}
                            </a>
                          ) : (
                            <span className="text-gray-200">{a.title}</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Holdings table */}
          <div className="bg-[#151821] rounded-xl border border-gray-800/40">
            <div className="px-5 py-4 border-b border-gray-800/40 flex items-center gap-4">
              <h2 className="text-sm font-medium text-gray-300">All Holdings</h2>
              <input
                type="text"
                placeholder="Filter by ticker, name, sector, country..."
                value={filter}
                onChange={e => setFilter(e.target.value)}
                className="ml-auto bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-1.5 text-sm text-gray-200 placeholder-gray-500 w-72 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
              <span className="text-gray-500 text-xs">{filtered.length.toLocaleString()} shown</span>
            </div>
            <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
              <table className="w-full text-sm">
                <thead className="sticky top-0 bg-[#151821] z-10">
                  <tr className="text-gray-400 text-xs uppercase tracking-wider">
                    <th className="text-left px-3 py-2.5 font-medium">#</th>
                    {([
                      ['Ticker', 'Ticker', 'left'],
                      ['Name', 'Name', 'left'],
                      [null, 'GuruFocus', 'left'],
                      ['Sector', 'Sector', 'left'],
                      ['Location', 'Location', 'left'],
                      ['Price', 'Price', 'right'],
                      ['Exchange', 'Exchange', 'left'],
                      ['Currency', 'Currency', 'left'],
                      ['Weight (%)', 'Weight', 'right'],
                      ['Market Value', 'Market Value', 'right'],
                    ] as const).map(([key, label, align]) => (
                      <th
                        key={label}
                        className={`text-${align} px-3 py-2.5 font-medium ${key ? 'cursor-pointer select-none hover:text-gray-200 transition-colors' : ''}`}
                        onClick={key ? () => {
                          if (sortCol === key) {
                            setSortAsc(!sortAsc);
                          } else {
                            setSortCol(key);
                            setSortAsc(true);
                          }
                        } : undefined}
                      >
                        {label}
                        {key && sortCol === key && (
                          <span className="ml-1 text-indigo-400">{sortAsc ? '\u25B2' : '\u25BC'}</span>
                        )}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-800/30">
                  {filtered.map((h, i) => (
                    <tr key={`${h.Ticker}-${i}`} className="hover:bg-white/[0.02]">
                      <td className="px-3 py-2.5 text-gray-500 font-mono text-xs">{i + 1}</td>
                      <td className="px-3 py-2.5 text-white font-mono font-medium">{h.Ticker}</td>
                      <td className="px-3 py-2.5 text-gray-200 max-w-[200px] truncate">{h.Name}</td>
                      <td className="px-3 py-2.5 text-xs">
                        {h.gurufocus_url ? (
                          <a
                            href={h.gurufocus_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-indigo-400 hover:text-indigo-300 transition-colors"
                          >
                            link
                          </a>
                        ) : (
                          <span className="text-gray-600">—</span>
                        )}
                      </td>
                      <td className="px-3 py-2.5 text-gray-400">{h.Sector}</td>
                      <td className="px-3 py-2.5 text-gray-400">{h.Location}</td>
                      <td className="px-3 py-2.5 text-gray-300 font-mono text-right">{fmtNum(h.Price)}</td>
                      <td className="px-3 py-2.5 text-gray-400 text-xs">{h.Exchange}</td>
                      <td className="px-3 py-2.5 text-gray-400 font-mono text-xs">{h.Currency}</td>
                      <td className="px-3 py-2.5 text-gray-300 font-mono text-right">{fmtNum(h['Weight (%)'])}%</td>
                      <td className="px-3 py-2.5 text-gray-300 font-mono text-right">{fmtMv(h['Market Value'])}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
