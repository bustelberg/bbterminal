'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import ProgressTimeline from './ProgressTimeline';
import { apiFetch } from '../../lib/apiFetch';
import { colorForSector } from '../../lib/sectorColors';
import { fmtTimestamp } from '../../lib/format';
import type { Column } from '../../lib/tableExport';
import TableDownloadButton from './TableDownloadButton';
import LoadingDots from './LoadingDots';
import { API_URL } from '../../lib/apiUrl';
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
  // Flatten the hierarchical sector/industry/company tree into one row per
  // company for the download. Walks the *currently filtered* tree so the
  // export matches what the user sees on screen.
  type FlatLeonteqRow = {
    sector: string;
    industry: string;
    name: string;
    ticker: string;
    isin: string;
    company_id: number | null;
    gurufocus_url: string;
  };

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

  const flatLeonteqRows = useMemo<FlatLeonteqRow[]>(() => {
    const out: FlatLeonteqRow[] = [];
    for (const sec of filtered) {
      for (const ind of sec.industries) {
        for (const c of ind.companies) {
          out.push({
            sector: sec.name,
            industry: ind.name,
            name: c.name,
            ticker: c.ticker ?? '',
            isin: c.isin ?? '',
            company_id: c.company_id,
            gurufocus_url: c.gurufocus_url ?? '',
          });
        }
      }
    }
    return out;
  }, [filtered]);
  const leonteqExportColumns = useMemo<Column<FlatLeonteqRow>[]>(() => [
    { key: 'sector', header: 'Sector', accessor: (r) => r.sector },
    { key: 'industry', header: 'Industry', accessor: (r) => r.industry },
    { key: 'name', header: 'Name', accessor: (r) => r.name },
    { key: 'ticker', header: 'Ticker', accessor: (r) => r.ticker },
    { key: 'isin', header: 'ISIN', accessor: (r) => r.isin },
    { key: 'company_id', header: 'Company ID', accessor: (r) => r.company_id ?? '' },
    { key: 'gurufocus_url', header: 'GuruFocus URL', accessor: (r) => r.gurufocus_url },
  ], []);

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
    <div className="min-h-screen bg-page text-fg">
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">Leonteq</h1>
        <p className="text-sm text-fg-subtle mt-1">
          Equities Leonteq lists as underlyings for their structured products. Scraped from{' '}
          <a
            href="https://structuredproducts-ch.leonteq.com/services/underlyings"
            target="_blank"
            rel="noopener noreferrer"
            className="text-accent-400 hover:underline"
          >
            structuredproducts-ch.leonteq.com
          </a>
          . Grouped by their sector → industry classification. Each industry maps to exactly one sector.
        </p>
      </div>

      <div className="px-8 py-6 space-y-6 max-w-6xl">
        {error && (
          <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3 text-sm text-neg-300">
            {error}
          </div>
        )}

        {/* Stats + refresh */}
        <div className="bg-card rounded-xl border border-neutral-800/40 px-5 py-4 flex items-start justify-between gap-4 flex-wrap">
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
            className="text-xs px-3 py-1.5 rounded-lg bg-accent-600 hover:bg-accent-500 disabled:opacity-50 disabled:cursor-not-allowed text-fg-strong transition-colors shrink-0"
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
              className="bg-card border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg placeholder-fg-subtle focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 focus:outline-none flex-1 min-w-[240px]"
            />
            <button
              type="button"
              onClick={() => setExpandedSectors(new Set(filtered.map((s) => s.name)))}
              className="text-xs text-fg-muted hover:text-fg transition-colors"
            >
              Expand all
            </button>
            <span className="text-fg-dim">·</span>
            <button
              type="button"
              onClick={() => setExpandedSectors(new Set())}
              className="text-xs text-fg-muted hover:text-fg transition-colors"
            >
              Collapse all
            </button>
            <div className="ml-auto">
              <TableDownloadButton
                rows={flatLeonteqRows}
                columns={leonteqExportColumns}
                filename="leonteq_underlyings"
                title={`Download ${flatLeonteqRows.length} Leonteq underlyings as CSV / XLSX`}
              />
            </div>
          </div>
        )}

        {loading && !data ? (
          <div className="text-sm text-fg-subtle"><LoadingDots label="Loading Leonteq universe" /></div>
        ) : !data || data.total_equities === 0 ? (
          <div className="bg-card rounded-xl border border-neutral-800/40 px-5 py-6 text-sm text-fg-muted">
            No equities scraped yet. Click <span className="text-fg">Refresh now</span> to scrape the Leonteq page.
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
                  className="bg-card rounded-xl border border-neutral-800/40 overflow-hidden"
                  style={{ borderLeftColor: color, borderLeftWidth: '3px' }}
                >
                  <button
                    type="button"
                    onClick={() => toggleSector(sec.name)}
                    className="w-full px-5 py-3 flex items-center gap-3 text-left hover:bg-overlay/[0.02] transition-colors"
                  >
                    <span className="text-fg-subtle font-mono text-xs w-4 shrink-0">{isOpen ? '▾' : '▸'}</span>
                    <span
                      className="inline-block w-2 h-2 rounded-full shrink-0"
                      style={{ background: color }}
                    />
                    <h3 className="text-sm font-medium text-fg-strong" style={{ color }}>
                      {sec.name}
                    </h3>
                    <span className="text-xs text-fg-subtle font-mono ml-auto">
                      {sec.industries.length} industr{sec.industries.length === 1 ? 'y' : 'ies'} · {sec.company_count} equities
                    </span>
                  </button>
                  {isOpen && (
                    <div className="border-t border-neutral-800/40 divide-y divide-neutral-800/30">
                      {sec.industries.map((ind) => (
                        <div key={ind.name} className="px-5 py-3">
                          <div className="flex items-baseline gap-3 mb-2">
                            <span className="text-xs font-medium text-fg-soft">{ind.name}</span>
                            <span className="text-[10px] text-fg-subtle font-mono">{ind.company_count}</span>
                          </div>
                          <div className="flex flex-wrap gap-1.5">
                            {ind.companies.map((c, idx) => (
                              // ISIN is unique per listing — different listings of the
                              // same company (BHP on LSE vs ASX, Shell on LSE vs NYSE,
                              // Samsung's share classes, …) collapse to one company_id
                              // but keep distinct ISINs. Using ISIN as the primary key
                              // prevents the React duplicate-key warning. idx is the
                              // last-resort fallback for the ~5 rows with no ISIN.
                              <CompanyChip
                                key={c.isin ?? `${c.company_id ?? 'noid'}-${c.ticker ?? c.name ?? ''}-${idx}`}
                                c={c}
                              />
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
              <div className="text-sm text-fg-subtle">No matches for &ldquo;{search}&rdquo;.</div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-page border border-neutral-800/40 rounded-lg px-3 py-2">
      <div className="text-[10px] uppercase tracking-wider text-fg-subtle">{label}</div>
      <div className="font-mono text-sm text-fg mt-0.5 truncate">{value}</div>
    </div>
  );
}

function CompanyChip({ c }: { c: Company }) {
  const inner = (
    <>
      <span className="text-fg truncate max-w-[220px]" title={c.name}>{c.name}</span>
      {c.ticker && <span className="text-fg-subtle font-mono text-[10px]">{c.ticker}</span>}
    </>
  );
  const className =
    'bg-page border border-neutral-800/60 rounded px-2 py-1 flex items-baseline gap-1.5 text-[11px] hover:border-accent-500/40 transition-colors';
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
        <span className="text-accent-400 text-[9px]">↗</span>
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
