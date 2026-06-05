'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import ProgressTimeline from './ProgressTimeline';
import { apiFetch } from '../../lib/apiFetch';
import type { Column } from '../../lib/tableExport';
import TableDownloadButton from './TableDownloadButton';
import LoadingDots from './LoadingDots';
import { API_URL } from '../../lib/apiUrl';
import { useApiData } from '../../lib/hooks/useApiData';
const TEMPLATE_KEY = 'ACWI';

type Summary = {
  template_key: string;
  label: string;
  description: string;
  earliest_date: string;
  universe_id: number | null;
  months_captured: number;
  earliest_captured_month: string | null;
  latest_captured_month: string | null;
  latest_membership_count: number;
  months: string[];
};

type MembershipRow = {
  company_id: number;
  ticker: string;
  company_name: string;
  exchange: string;
  sector: string | null;
  gurufocus_url: string | null;
};

/** Top section of /acwi: the canonical, template-managed ACWI universe.
 * Date scrubber lets the user inspect membership at any captured month;
 * Refresh button kicks off `POST /api/universe-templates/ACWI/refresh`
 * over SSE so the user can manually trigger an update instead of waiting
 * for the next pipeline tick.
 *
 * The page's bottom section (iShares fund holdings + MSCI announcements
 * + net additions) is left intact for diagnostics — those are the
 * upstream data sources the reconstruction reads from. */
export default function AcwiCanonicalView() {
  const [summary, setSummary] = useState<Summary | null>(null);
  const [summaryLoading, setSummaryLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedMonth, setSelectedMonth] = useState<string>('');
  const [search, setSearch] = useState('');
  const [membership, setMembership] = useState<MembershipRow[]>([]);
  const [membershipLoading, setMembershipLoading] = useState(false);
  const [refreshLog, setRefreshLog] = useState<string[]>([]);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshResult, setRefreshResult] = useState<{ ok: boolean; message: string } | null>(null);

  const loadSummary = useCallback(async () => {
    setSummaryLoading(true);
    try {
      const r = await fetch(`${API_URL}/api/universe-templates/${TEMPLATE_KEY}`);
      if (!r.ok) {
        setError(`Failed to load (${r.status})`);
        return;
      }
      const data = (await r.json()) as Summary;
      setSummary(data);
      // Default to the most recent captured month
      if (data.months.length > 0 && !selectedMonth) {
        setSelectedMonth(data.months[data.months.length - 1]);
      }
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSummaryLoading(false);
    }
  }, [selectedMonth]);

  useEffect(() => {
    void loadSummary();
  }, [loadSummary]);

  // Fetch membership when the selected month changes.
  useEffect(() => {
    if (!selectedMonth) return;
    let cancelled = false;
    setMembershipLoading(true);
    fetch(`${API_URL}/api/universe-templates/${TEMPLATE_KEY}/membership?date=${selectedMonth}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((data: { membership: MembershipRow[] }) => {
        if (cancelled) return;
        setMembership(data.membership || []);
      })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : String(e)); })
      .finally(() => { if (!cancelled) setMembershipLoading(false); });
    return () => { cancelled = true; };
  }, [selectedMonth]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return membership;
    return membership.filter(
      (r) =>
        r.ticker.toLowerCase().includes(q) ||
        r.company_name.toLowerCase().includes(q) ||
        r.exchange.toLowerCase().includes(q) ||
        (r.sector ?? '').toLowerCase().includes(q),
    );
  }, [membership, search]);

  const membershipExportColumns = useMemo<Column<MembershipRow>[]>(() => [
    { key: 'ticker', header: 'Ticker', accessor: (r) => r.ticker },
    { key: 'company_name', header: 'Name', accessor: (r) => r.company_name },
    { key: 'exchange', header: 'Exchange', accessor: (r) => r.exchange },
    { key: 'sector', header: 'Sector', accessor: (r) => r.sector ?? '' },
    { key: 'company_id', header: 'Company ID', accessor: (r) => r.company_id },
    { key: 'gurufocus_url', header: 'GuruFocus URL', accessor: (r) => r.gurufocus_url ?? '' },
  ], []);

  // SSE refresh — same event shape the pipeline phase emits.
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
            // Non-JSON keepalive lines — ignore.
          }
        }
      }
      // Re-pull summary so months + count update after a successful refresh.
      await loadSummary();
    } catch (e) {
      setRefreshResult({ ok: false, message: e instanceof Error ? e.message : String(e) });
    } finally {
      setRefreshing(false);
    }
  }, [refreshing, loadSummary]);

  if (summaryLoading && !summary) {
    return (
      <div className="bg-card rounded-xl border border-neutral-800/40 px-5 py-4 text-sm text-fg-subtle">
        <LoadingDots label="Loading canonical ACWI universe" />
      </div>
    );
  }
  if (!summary) {
    return (
      <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3 text-sm text-neg-300">
        {error ?? 'Failed to load.'}
      </div>
    );
  }

  const hasData = summary.months_captured > 0;

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40">
      <div className="px-5 py-4 border-b border-neutral-800/40 flex items-start justify-between gap-4 flex-wrap">
        <div className="min-w-0">
          <h2 className="text-sm font-medium text-fg-strong">
            ACWI Universe
            <span className="ml-2 text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-accent-500/15 text-accent-300 border-accent-500/30">
              template-managed
            </span>
          </h2>
          <p className="text-xs text-fg-subtle mt-1 leading-relaxed max-w-3xl">
            {summary.description}{' '}
            Data starts <span className="font-mono text-fg-muted">{summary.earliest_date}</span>.
            Refreshed automatically on every pipeline tick (weekly + monthly) — no manual creation needed.
          </p>
          <XlsAgeBadge />
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <a
            href={`${API_URL}/api/universe-templates/${TEMPLATE_KEY}/all-companies.csv`}
            // `download` instructs the browser to save instead of
            // navigate. The server already sets Content-Disposition with
            // a sensible filename, so we leave the attribute valueless.
            download
            className={`text-xs px-3 py-1.5 rounded-lg border border-neutral-700 text-fg hover:bg-overlay/5 transition-colors ${hasData ? '' : 'opacity-50 pointer-events-none'}`}
            title={hasData ? 'Download every company ever in the index as CSV' : 'No data yet — refresh first'}
          >
            Download CSV
          </a>
          <button
            type="button"
            onClick={() => void triggerRefresh()}
            disabled={refreshing}
            className="text-xs px-3 py-1.5 rounded-lg bg-accent-600 hover:bg-accent-500 disabled:opacity-50 disabled:cursor-not-allowed text-fg-strong transition-colors"
          >
            {refreshing ? 'Refreshing…' : 'Refresh now'}
          </button>
        </div>
      </div>

      {(refreshLog.length > 0 || refreshResult) && (
        <div className="px-5 py-3 border-b border-neutral-800/40">
          <ProgressTimeline
            steps={[]}
            log={refreshLog}
            doneSummary={refreshResult?.ok ? refreshResult.message : null}
            errorMessage={refreshResult && !refreshResult.ok ? refreshResult.message : null}
            running={refreshing}
            defaultLogOpen
            title="Refresh progress"
            onDismiss={() => { setRefreshLog([]); setRefreshResult(null); }}
          />
        </div>
      )}

      <div className="px-5 py-4 grid gap-3 grid-cols-2 sm:grid-cols-4 text-xs">
        <Stat label="Months captured" value={summary.months_captured.toString()} />
        <Stat label="Earliest month" value={summary.earliest_captured_month ?? '—'} />
        <Stat label="Latest month" value={summary.latest_captured_month ?? '—'} />
        <Stat label="Latest count" value={summary.latest_membership_count.toString()} />
      </div>

      {!hasData ? (
        <div className="px-5 py-6 text-sm text-fg-muted border-t border-neutral-800/40">
          No memberships captured yet. Click <span className="text-fg">Refresh now</span> to do an initial build (it&apos;ll reconstruct ~290 monthly snapshots from {summary.earliest_date} to today — takes ~30-60s).
        </div>
      ) : (
        <div className="border-t border-neutral-800/40">
          <div className="px-5 py-3 flex items-center gap-3 flex-wrap">
            <label className="text-xs text-fg-muted">Month</label>
            <select
              value={selectedMonth}
              onChange={(e) => setSelectedMonth(e.target.value)}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-xs text-fg font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 focus:outline-none"
            >
              {summary.months.map((m) => (
                <option key={m} value={m}>{m}</option>
              ))}
            </select>
            <input
              type="search"
              placeholder="Search by ticker, name, exchange, sector…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-xs text-fg placeholder-fg-subtle focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 focus:outline-none flex-1 min-w-[200px]"
            />
            <span className="text-xs text-fg-subtle font-mono">
              {membershipLoading ? <LoadingDots label="loading" /> : `${filtered.length} / ${membership.length}`}
            </span>
            <TableDownloadButton
              rows={filtered}
              columns={membershipExportColumns}
              filename={`acwi_${selectedMonth || 'membership'}`}
              title={`Download ${filtered.length} ACWI members as CSV / XLSX`}
            />
          </div>
          <div className="max-h-[600px] overflow-auto">
            <table className="w-full text-xs">
              <thead className="text-fg-subtle text-[10px] uppercase sticky top-0 bg-card">
                <tr className="border-b border-neutral-800/40">
                  <th className="text-left px-3 py-1.5 font-medium">Ticker</th>
                  <th className="text-left px-3 py-1.5 font-medium">Name</th>
                  <th className="text-left px-3 py-1.5 font-medium">Exchange</th>
                  <th className="text-left px-3 py-1.5 font-medium">Sector</th>
                  <th className="text-left px-3 py-1.5 font-medium">GuruFocus</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((r) => (
                  <tr key={r.company_id} className="border-b border-neutral-800/20 hover:bg-overlay/[0.02]">
                    <td className="px-3 py-1.5 font-mono text-fg">{r.ticker || '—'}</td>
                    <td className="px-3 py-1.5 text-fg-soft truncate max-w-[280px]">{r.company_name || '—'}</td>
                    <td className="px-3 py-1.5 font-mono text-fg-subtle">{r.exchange || '—'}</td>
                    <td className="px-3 py-1.5 text-fg-muted">{r.sector || '—'}</td>
                    <td className="px-3 py-1.5">
                      {r.gurufocus_url ? (
                        <a
                          href={r.gurufocus_url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-accent-400 hover:underline font-mono"
                        >
                          link
                        </a>
                      ) : <span className="text-fg-faint">—</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-page border border-neutral-800/40 rounded-lg px-3 py-2">
      <div className="text-[10px] uppercase tracking-wider text-fg-subtle">{label}</div>
      <div className="font-mono text-sm text-fg mt-0.5">{value}</div>
    </div>
  );
}


/** Bundled iShares ACWI XLS age. iShares blocks automated downloads,
 * so the file lives in the repo and updates only when someone commits
 * a fresh one. Stale = the reconstruction is operating against
 * yesterday's holdings. Shown only when actually stale (>= 14 days)
 * to avoid cluttering the page on a healthy install. */
function XlsAgeBadge() {
  const { data: age } = useApiData<number | null, { available?: boolean; age_days?: number } | null>(
    '/api/acwi/xls-age',
    {
      transform: (d) => (d?.available && d.age_days != null ? d.age_days : null),
      fallbackData: null,
    },
  );
  if (age == null || age < 14) return null;
  return (
    <p className="text-xs text-warn-300 mt-2 max-w-3xl">
      ⚠ Bundled iShares XLS is <span className="font-mono">{age}</span> day{age === 1 ? '' : 's'} old.
      iShares blocks automated downloads, so the reconstruction runs against this stale file until
      someone manually commits a fresh <span className="font-mono">iShares-MSCI-ACWI-ETF_fund.xls</span>.
    </p>
  );
}
