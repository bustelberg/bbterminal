'use client';
import { useEffect, useState } from 'react';
import { API_URL } from '../../lib/apiUrl';
import { apiFetch } from '../../lib/apiFetch';
import type { PortfolioConnection } from './ConnectionHoldingsView';
type Month = { month: string; leonteq_pct: number | null; airs_pct: number | null; difference_pp: number | null; airs_as_of: string | null };
export type MonthlyData = { success: boolean; status?: string; months?: Month[]; ytd?: Record<string, { leonteq_pct: number | null; airs_pct: number | null; difference_pp: number | null }>; retrieved_at?: string; refresh_error?: string; refresh_errors?: { source: string; code: string }[]; saved_data?: boolean };
export function monthlyFailure(data: MonthlyData, httpStatus?: number): string {
  const reasons: Record<string, string> = {
    airs_busy: 'AIRS is busy with another refresh. Retry shortly.', timeout: 'Request timed out. Retry.',
    login_rejected: 'Login rejected. Check the configured credentials.', missing_credentials: 'Login credentials are missing.',
    verification_required: 'Login requires additional verification.', busy: 'Another Leonteq login is running. Retry shortly.',
    performance_failed: 'Could not read Details → Performance Metrics.', portfolio_not_found: 'Portfolio was not found in AMC Indices.',
    browser_error: 'Portal browser connection failed.', browser_unavailable: 'Portal browser is unavailable.',
    airs_refresh_failed: 'Performance report download, parsing or storage failed.',
    airs_download_failed: 'Could not download the performance report. Check AIRS login/access and retry.',
    airs_parse_failed: 'The downloaded performance report could not be read.',
    airs_empty_performance: 'The performance report contains no monthly returns.',
    airs_store_failed: 'The performance report was downloaded but could not be saved to the database.',
  };
  if (data.refresh_errors?.length) return data.refresh_errors.map(({ source, code }) => `${source}: ${reasons[code] ?? `Refresh failed (${code}).`}`).join(' ');
  const code = data.refresh_error ?? data.status;
  if (code && code !== 'success') return `${code.startsWith('airs_') ? 'AIRS' : 'Leonteq'}: ${reasons[code] ?? `Refresh failed (${code}).`}`;
  if (httpStatus === 401 || httpStatus === 403) return `Access denied (HTTP ${httpStatus}). Sign in as an admin.`;
  if (httpStatus) return `Monthly returns request failed (HTTP ${httpStatus}).`;
  return 'No valid monthly returns were returned.';
}
const pct = (value: number | null, suffix = '%') => value === null ? '—' : `${value > 0 ? '+' : ''}${value.toFixed(2)}${suffix}`;
const tone = (value: number | null) => value === null ? 'text-fg-muted/50' : value > 0 ? 'text-pos-300' : value < 0 ? 'text-neg-300' : 'text-fg-muted';
let queue: Promise<unknown> = Promise.resolve();
export function queueMonthly(request: () => Promise<Response>) {
  const result = queue.then(request);
  queue = result.catch(() => undefined);
  return result;
}
export default function MonthlyReturns({ link, fresh, refreshState, refreshError, errorReport, onRefresh, refreshDisabled }: { link: PortfolioConnection; fresh?: MonthlyData; refreshState?: 'Queued' | 'Refreshing' | 'Updated' | 'Failed'; refreshError?: string; errorReport?: unknown; onRefresh?: () => void; refreshDisabled?: boolean }) {
  const [data, setData] = useState<MonthlyData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [diagnostic, setDiagnostic] = useState<unknown>(null);
  const [copyStatus, setCopyStatus] = useState('');
  const [year, setYear] = useState('');
  useEffect(() => {
    let active = true;
    setCopyStatus('');
    if (fresh) { setData(fresh); setLoading(false); setError(null); return; }
    setLoading(true); setError(null); setDiagnostic(null);
    const request = queueMonthly(() => apiFetch(`${API_URL}/api/leonteq/portfolios/monthly-performance`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: link.leonteq_name, airs_portfolio_id: link.airs_portfolio_id, refresh: false }),
    }));
    void request.then(async (response) => {
      const result = await response.json().catch(() => ({})) as MonthlyData;
      if (active) setDiagnostic({ http_status: response.status, response: { success: result.success, status: result.status,
        refresh_error: result.refresh_error, refresh_errors: result.refresh_errors, saved_data: result.saved_data,
        detail: (result as MonthlyData & { detail?: unknown }).detail } });
      if (!response.ok || !result.success || !result.months) throw new Error(monthlyFailure(result, response.ok ? undefined : response.status));
      if (active) setData(result);
    }).catch((cause) => { if (active) setError(cause instanceof Error ? cause.message : 'Network error. Check the connection and retry.'); })
      .finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, [link.leonteq_name, link.airs_portfolio_id, fresh]);
  const years = Array.from(new Set(data?.months?.map((month) => month.month.slice(0, 4)) ?? [])).sort().reverse();
  const selectedYear = years.includes(year) ? year : years[0];
  const values = new Map(data?.months?.map((row) => [row.month, row]));
  const failure = refreshError ?? error ?? (data?.refresh_error ? monthlyFailure(data) : null);
  const badge = refreshState ?? (failure ? 'Failed' : undefined);
  async function copyError() {
    try {
      await navigator.clipboard.writeText(JSON.stringify({
        copied_at: new Date().toISOString(), operation: 'monthly_returns',
        leonteq_portfolio: link.leonteq_name, airs_portfolio: link.airs_name, airs_portfolio_id: link.airs_portfolio_id,
        message: failure, saved_data_visible: Boolean(data?.months?.length),
        request: { endpoint: '/api/leonteq/portfolios/monthly-performance', refresh: Boolean(refreshState) },
        diagnostics: errorReport ?? diagnostic ?? { refresh_errors: data?.refresh_errors, refresh_error: data?.refresh_error },
      }, null, 2));
      setCopyStatus('Copied');
    } catch { setCopyStatus('Copy failed'); }
  }
  return <section className={`overflow-hidden rounded-xl border ${refreshState === 'Refreshing' ? 'border-accent-600 ring-1 ring-accent-600/40' : 'border-neutral-800/40'}`} aria-label="Monthly returns" aria-busy={loading || refreshState === 'Refreshing'}>
    <div className="flex flex-wrap items-center justify-between gap-3 bg-elevated/40 px-4 py-4">
      <div><h3 className="font-semibold">{link.leonteq_name}</h3><p className="mt-1 text-xs text-fg-muted">AIRS · {link.airs_name}</p></div>
      <div className="flex items-center gap-2">
        {onRefresh && <button type="button" aria-label={`Refresh returns for ${link.leonteq_name}`} disabled={refreshDisabled || loading} onClick={onRefresh} className="rounded-lg border border-neutral-800/40 px-3 py-1.5 text-xs text-fg-muted hover:bg-overlay/5 disabled:opacity-50">Refresh</button>}
        {failure && <button type="button" onClick={() => void copyError()} className="rounded-lg border border-neutral-800/40 px-3 py-1.5 text-xs text-fg-muted hover:bg-overlay/5">{copyStatus || 'Copy .json'}</button>}
        {badge && <span role={badge === 'Failed' ? 'alert' : 'status'} className={`inline-flex items-center gap-2 rounded-full px-3 py-1 text-xs font-medium ${badge === 'Refreshing' ? 'bg-accent-600/15 text-accent-300' : badge === 'Updated' ? 'bg-green-500/10 text-pos-300' : badge === 'Failed' ? 'bg-red-500/10 text-neg-300' : 'bg-elevated text-fg-muted'}`}>
          {badge === 'Refreshing' && <span aria-hidden="true" className="h-3 w-3 animate-spin rounded-full border-2 border-current border-r-transparent" />}{badge}
        </span>}
        {years.length > 0 && <select aria-label="Returns year" value={selectedYear} onChange={(event) => setYear(event.target.value)} className="rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-sm">{years.map((value) => <option key={value}>{value}</option>)}</select>}
      </div>
    </div>
    {loading && <p role="status" className="px-4 py-2 text-xs text-fg-muted">Loading…</p>}
    <div className="overflow-x-auto"><table className="w-full text-sm">
      <caption className="sr-only">Monthly return comparison · {selectedYear}</caption>
      <thead className="bg-elevated/60 text-[11px] text-fg-muted"><tr><th scope="col" className="px-4 py-3 text-left font-medium">Source</th>{Array.from({ length: 12 }, (_, index) => <th scope="col" className="px-3 py-3 text-right font-medium" key={index}>{new Intl.DateTimeFormat('en', { month: 'short', timeZone: 'UTC' }).format(new Date(Date.UTC(2020, index, 1)))}</th>)}<th scope="col" className="border-l border-neutral-800/40 bg-elevated px-4 py-3 text-right font-semibold text-fg">YTD</th></tr></thead>
      <tbody>{(['AIRS', 'Leonteq', 'Difference (pp)'] as const).map((label) => <tr key={label} className={`border-t border-neutral-800/30 ${label === 'Difference (pp)' ? 'bg-elevated/40' : 'hover:bg-elevated/20'}`}><th scope="row" className="whitespace-nowrap px-4 py-3 text-left text-xs font-medium" title={label === 'Difference (pp)' ? 'Leonteq minus AIRS, in percentage points' : label === 'AIRS' ? link.airs_name ?? '' : link.leonteq_name}><span className="inline-flex items-center gap-2"><span aria-hidden="true" className={`h-1.5 w-1.5 rounded-full ${label === 'AIRS' ? 'bg-sky-400' : label === 'Leonteq' ? 'bg-violet-400' : 'bg-neutral-500'}`} />{label === 'Difference (pp)' ? 'Difference' : label}</span></th>{Array.from({ length: 12 }, (_, index) => {
        const month = values.get(`${selectedYear}-${String(index + 1).padStart(2, '0')}`);
        const value = month ? label === 'Leonteq' ? month.leonteq_pct : label === 'AIRS' ? month.airs_pct : month.difference_pp : null;
        return <td key={index} className={`whitespace-nowrap px-3 py-3 text-right text-xs tabular-nums ${tone(value)}`} title={month?.airs_as_of ? `AIRS as of ${month.airs_as_of}` : undefined}>{pct(value, label === 'Difference (pp)' ? ' pp' : '%')}</td>;
      })}<td className={`whitespace-nowrap border-l border-neutral-800/40 bg-elevated/50 px-4 py-3 text-right text-sm font-semibold tabular-nums ${tone(data?.ytd?.[selectedYear]?.[label === 'AIRS' ? 'airs_pct' : label === 'Leonteq' ? 'leonteq_pct' : 'difference_pp'] ?? null)}`}>{pct(data?.ytd?.[selectedYear]?.[label === 'AIRS' ? 'airs_pct' : label === 'Leonteq' ? 'leonteq_pct' : 'difference_pp'] ?? null, label === 'Difference (pp)' ? ' pp' : '%')}</td></tr>)}</tbody>
    </table></div>
  </section>;
}
