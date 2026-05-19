'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import ProgressTimeline from './ProgressTimeline';
import { apiFetch } from '../../lib/apiFetch';
import { colorForSector } from '../../lib/sectorColors';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';
const TEMPLATE_KEY = 'LEONTEQ';

type Company = {
  name: string;
  ticker: string | null;
  isin: string | null;
  gurufocus_url: string | null;
  company_id: number | null;
};

type Industry = {
  name: string;
  company_count: number;
  companies: Company[];
};

type Sector = {
  name: string;
  company_count: number;
  industries: Industry[];
};

type Overview = {
  total_equities: number;
  unique_sectors: number;
  unique_industries: number;
  scraped_at: string | null;
  sectors: Sector[];
};

function fmtTimestamp(iso: string | null): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString(undefined, {
      year: 'numeric', month: 'short', day: '2-digit',
      hour: '2-digit', minute: '2-digit',
    });
  } catch {
    return iso;
  }
}

/** /leonteq — hierarchical view of Leonteq's underlying equities,
 * grouped by sector → industry → company. Each company carries a
 * direct GuruFocus link when we resolved them to a known company row.
 * Refresh kicks off the same SSE template-refresh flow the pipeline
 * uses, so progress is live. */
export default function LeonteqUniverse() {
  const [data, setData] = useState<Overview | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [expandedSectors, setExpandedSectors] = useState<Set<string>>(new Set());

  const [refreshing, setRefreshing] = useState(false);
  const [refreshLog, setRefreshLog] = useState<string[]>([]);
  const [refreshResult, setRefreshResult] = useState<{ ok: boolean; message: string } | null>(null);

  const load = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/api/leonteq/overview`);
      if (!r.ok) {
        setError(`Failed to load (${r.status})`);
        return;
      }
      setData((await r.json()) as Overview);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  // Filtered view — applies a single search across sector / industry /
  // company-name. The sector is included regardless if the query
  // matches the sector name; otherwise only matching industries +
  // companies survive.
  const filtered = useMemo<Sector[]>(() => {
    if (!data) return [];
    const q = search.trim().toLowerCase();
    if (!q) return data.sectors;
    return data.sectors
      .map((sec): Sector | null => {
        const secMatch = sec.name.toLowerCase().includes(q);
        const industries = sec.industries
          .map((ind): Industry | null => {
            const indMatch = ind.name.toLowerCase().includes(q);
            const companies = ind.companies.filter((c) =>
              c.name.toLowerCase().includes(q) ||
              (c.ticker ?? '').toLowerCase().includes(q) ||
              (c.isin ?? '').toLowerCase().includes(q),
            );
            if (secMatch || indMatch || companies.length > 0) {
              return {
                ...ind,
                companies: secMatch || indMatch ? ind.companies : companies,
                company_count: secMatch || indMatch ? ind.company_count : companies.length,
              };
            }
            return null;
          })
          .filter((x): x is Industry => x !== null);
        if (secMatch || industries.length > 0) {
          return {
            ...sec,
            industries,
            company_count: industries.reduce((s, i) => s + i.company_count, 0),
          };
        }
        return null;
      })
      .filter((x): x is Sector => x !== null);
  }, [data, search]);

  // SSE refresh — same event shape as /acwi's canonical-refresh path.
  const triggerRefresh = useCallback(async () => {
    if (refreshing) return;
    setRefreshing(true);
    setRefreshLog([]);
    setRefreshResult(null);
    try {
      const resp = await apiFetch(`${API_URL}/api/universe-templates/${TEMPLATE_KEY}/refresh`, {
        method: 'POST',
        headers: { 'Accept': 'text/event-stream' },
      });
      if (!resp.ok || !resp.body) {
        setRefreshResult({ ok: false, message: `Refresh failed (${resp.status})` });
        return;
      }
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split('\n\n');
        buffer = parts.pop() ?? '';
        for (const part of parts) {
          const lines = part.split('\n').filter((l) => l.startsWith('data: '));
          if (!lines.length) continue;
          const payload = lines.map((l) => l.slice(6)).join('\n');
          try {
            const evt = JSON.parse(payload);
            if (evt.type === 'progress' && evt.message) {
              setRefreshLog((l) => [...l, evt.message]);
            } else if (evt.type === 'done') {
              setRefreshResult({ ok: true, message: evt.message });
            } else if (evt.type === 'error') {
              setRefreshResult({ ok: false, message: evt.message });
            }
          } catch {
            // Non-JSON keepalive — ignore.
          }
        }
      }
      await load();
    } catch (e) {
      setRefreshResult({ ok: false, message: e instanceof Error ? e.message : String(e) });
    } finally {
      setRefreshing(false);
    }
  }, [refreshing, load]);

  const toggleSector = (name: string) => {
    setExpandedSectors((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  return (
    <div className="min-h-screen bg-[#0f1117] text-gray-200">
      <div className="px-8 py-5 border-b border-gray-800/40">
        <h1 className="text-xl font-semibold text-white">Leonteq Underlyings</h1>
        <p className="text-sm text-gray-500 mt-1">
          Equities Leonteq lists as underlyings for their structured products. Scraped from{' '}
          <a
            href="https://structuredproducts-ch.leonteq.com/services/underlyings"
            target="_blank"
            rel="noopener noreferrer"
            className="text-indigo-400 hover:underline"
          >
            structuredproducts-ch.leonteq.com
          </a>
          . Grouped by their sector → industry classification. Each industry maps to exactly one sector.
        </p>
      </div>

      <div className="px-8 py-6 space-y-6 max-w-6xl">
        {error && (
          <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-sm text-rose-300">
            {error}
          </div>
        )}

        {/* Stats + refresh */}
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-5 py-4 flex items-start justify-between gap-4 flex-wrap">
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs flex-1">
            <Stat label="Equities" value={data ? String(data.total_equities) : '—'} />
            <Stat label="Sectors" value={data ? String(data.unique_sectors) : '—'} />
            <Stat label="Industries" value={data ? String(data.unique_industries) : '—'} />
            <Stat label="Last scraped" value={data ? fmtTimestamp(data.scraped_at) : '—'} />
          </div>
          <button
            type="button"
            onClick={() => void triggerRefresh()}
            disabled={refreshing}
            className="text-xs px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white transition-colors shrink-0"
          >
            {refreshing ? 'Scraping…' : 'Refresh now'}
          </button>
        </div>

        {(refreshLog.length > 0 || refreshResult) && (
          <ProgressTimeline
            steps={[]}
            log={refreshLog}
            doneSummary={refreshResult?.ok ? refreshResult.message : null}
            errorMessage={refreshResult && !refreshResult.ok ? refreshResult.message : null}
            running={refreshing}
            defaultLogOpen
            title="Scrape progress"
            onDismiss={() => { setRefreshLog([]); setRefreshResult(null); }}
          />
        )}

        {/* Search */}
        {data && data.total_equities > 0 && (
          <div className="flex items-center gap-3 flex-wrap">
            <input
              type="search"
              placeholder="Search sectors, industries, tickers, ISINs…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="bg-[#151821] border border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-200 placeholder-gray-500 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 focus:outline-none flex-1 min-w-[240px]"
            />
            <button
              type="button"
              onClick={() => setExpandedSectors(new Set(filtered.map((s) => s.name)))}
              className="text-xs text-gray-400 hover:text-gray-200 transition-colors"
            >
              Expand all
            </button>
            <span className="text-gray-700">·</span>
            <button
              type="button"
              onClick={() => setExpandedSectors(new Set())}
              className="text-xs text-gray-400 hover:text-gray-200 transition-colors"
            >
              Collapse all
            </button>
          </div>
        )}

        {loading && !data ? (
          <div className="text-sm text-gray-500">Loading Leonteq universe…</div>
        ) : !data || data.total_equities === 0 ? (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-5 py-6 text-sm text-gray-400">
            No equities scraped yet. Click <span className="text-gray-200">Refresh now</span> to scrape the Leonteq page.
            The first scrape takes ~30-60 seconds (headless Chromium has to load the SPA).
          </div>
        ) : (
          <div className="space-y-3">
            {filtered.map((sec, secIdx) => {
              const color = colorForSector(sec.name, secIdx);
              const isOpen = expandedSectors.has(sec.name) || !!search.trim();
              return (
                <div
                  key={sec.name}
                  className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden"
                  style={{ borderLeftColor: color, borderLeftWidth: '3px' }}
                >
                  <button
                    type="button"
                    onClick={() => toggleSector(sec.name)}
                    className="w-full px-5 py-3 flex items-center gap-3 text-left hover:bg-white/[0.02] transition-colors"
                  >
                    <span className="text-gray-500 font-mono text-xs w-4 shrink-0">{isOpen ? '▾' : '▸'}</span>
                    <span
                      className="inline-block w-2 h-2 rounded-full shrink-0"
                      style={{ background: color }}
                    />
                    <h3 className="text-sm font-medium text-white" style={{ color }}>
                      {sec.name}
                    </h3>
                    <span className="text-xs text-gray-500 font-mono ml-auto">
                      {sec.industries.length} industr{sec.industries.length === 1 ? 'y' : 'ies'} · {sec.company_count} equities
                    </span>
                  </button>
                  {isOpen && (
                    <div className="border-t border-gray-800/40 divide-y divide-gray-800/30">
                      {sec.industries.map((ind) => (
                        <div key={ind.name} className="px-5 py-3">
                          <div className="flex items-baseline gap-3 mb-2">
                            <span className="text-xs font-medium text-gray-300">{ind.name}</span>
                            <span className="text-[10px] text-gray-500 font-mono">{ind.company_count}</span>
                          </div>
                          <div className="flex flex-wrap gap-1.5">
                            {ind.companies.map((c) => (
                              <CompanyChip key={`${c.company_id ?? c.isin ?? c.name}-${c.ticker ?? ''}`} c={c} />
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
            {filtered.length === 0 && (
              <div className="text-sm text-gray-500">No matches for &ldquo;{search}&rdquo;.</div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-[#0f1117] border border-gray-800/40 rounded-lg px-3 py-2">
      <div className="text-[10px] uppercase tracking-wider text-gray-500">{label}</div>
      <div className="font-mono text-sm text-gray-200 mt-0.5 truncate">{value}</div>
    </div>
  );
}

function CompanyChip({ c }: { c: Company }) {
  const inner = (
    <>
      <span className="text-gray-200 truncate max-w-[220px]" title={c.name}>{c.name}</span>
      {c.ticker && <span className="text-gray-500 font-mono text-[10px]">{c.ticker}</span>}
    </>
  );
  const className =
    'bg-[#0f1117] border border-gray-800/60 rounded px-2 py-1 flex items-baseline gap-1.5 text-[11px] hover:border-indigo-500/40 transition-colors';
  if (c.gurufocus_url) {
    return (
      <a
        href={c.gurufocus_url}
        target="_blank"
        rel="noopener noreferrer"
        className={className}
        title={`${c.name}${c.isin ? ` · ${c.isin}` : ''} — open on GuruFocus`}
      >
        {inner}
        <span className="text-indigo-400 text-[9px]">↗</span>
      </a>
    );
  }
  // No GuruFocus URL — couldn't reconcile to a company. Still show the
  // chip but unlinked (greyed) so the user knows what's in the scrape.
  return (
    <span
      className={`${className} opacity-60`}
      title={`${c.name}${c.isin ? ` · ${c.isin}` : ''} — no GuruFocus link (not in our company table)`}
    >
      {inner}
    </span>
  );
}
