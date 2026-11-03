'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import type { JobToast } from '../../../lib/stores/jobs';

type Event = { seq: number; kind: string; message: string; elapsed_ms?: number;
  profile?: unknown; details?: unknown; [key: string]: unknown };

type Audit = {
  companies: number; label: string; cadence: string;
  companies_checked?: { company_id: number; company?: string; ticker?: string; exchange?: string }[];
  present_companies_by_metric: Record<string, number>;
  statements_to_fetch: number[]; estimates_to_fetch: number[];
  unavailable: { company_id: number; company?: string; reason: string }[];
  selection?: { company_id: number; company?: string; ticker?: string; exchange?: string;
    missing_metrics: string[]; statements: string; estimates: string }[];
  raw_reporting_by_year: { metric: string; year: number; companies: number }[];
  display_policy?: string;
};
type Receipt = { feed: string; receipt: { attempted?: number; loaded?: number; failed?: number;
  deferred?: number; rows?: number; calls?: number; outcomes?: { company_id: number; company: string;
    ticker?: string; exchange?: string; status: string; reason?: string; rows: number; calls: number }[] } };

export function BenchmarkOverview({ events }: { events: Event[] }) {
  const audits = events.map(e => e.details as Audit | undefined).filter((d): d is Audit => !!d?.present_companies_by_metric);
  const audit = audits.at(-1);
  const planned = audits[0];
  const timing = [...events].reverse().find(e => e.profile && typeof e.profile === 'object'
    && 'stored_data_audit' in e.profile)?.profile as Record<string, number> | undefined;
  const receipts = events.map(e => e.details as Receipt | undefined).filter((d): d is Receipt => !!d?.receipt);
  if (!audit) return <p className="mb-3">Reading benchmark membership and stored figures. The request plan appears after this check.</p>;
  const periods = new Map<string, Record<string, number>>();
  for (const p of audit.raw_reporting_by_year) {
    const row = periods.get(String(p.year)) ?? {};
    row[p.metric] = Math.max(row[p.metric] ?? 0, p.companies);
    periods.set(String(p.year), row);
  }
  return <section className="mb-4 space-y-3 text-fg">
    <div className="font-medium">{audit.label} · {audit.cadence} · {audit.companies.toLocaleString()} companies checked</div>
    <div className="grid grid-cols-2 gap-2">
      <div className="rounded border border-neutral-800 p-2">Statement requests: {planned.statements_to_fetch.length.toLocaleString()}</div>
      <div className="rounded border border-neutral-800 p-2">Estimate requests: {planned.estimates_to_fetch.length.toLocaleString()}</div>
      <div className="rounded border border-neutral-800 p-2">Not eligible: {audit.unavailable.length.toLocaleString()}</div>
      <div className="rounded border border-neutral-800 p-2">Companies with gaps: {audit.selection?.length.toLocaleString() ?? '—'}</div>
    </div>
    <p>{audit.display_policy}</p>
    {timing && <table className="w-full text-left"><caption className="text-left font-medium">Stage timings</caption>
      <tbody>{Object.entries(timing).map(([stage, ms]) => <tr key={stage}><td>{stage.replaceAll('_', ' ')}</td>
        <td className="text-right tabular-nums">{(ms / 1000).toFixed(2)}s</td></tr>)}</tbody></table>}
    <p>Counts below mean at least one stored value since 2017. Each chart separately checks its metric, reporting period, required inputs and usable weights. Missing values are excluded, never replaced with zero. Smaller available samples do not represent the full index.</p>
    <table className="w-full text-left"><caption className="mb-1 text-left font-medium">Stored figures by metric</caption>
      <thead><tr><th>Metric</th><th className="text-right">Companies</th><th className="text-right">Missing</th></tr></thead>
      <tbody>{Object.entries(audit.present_companies_by_metric).map(([metric, count]) => <tr key={metric} className="border-t border-neutral-800/60">
        <td>{metric.replaceAll('_', ' ')}</td><td className="text-right tabular-nums">{count}</td><td className="text-right tabular-nums">{audit.companies - count}</td>
      </tr>)}</tbody></table>
    <details><summary className="cursor-pointer">Reporting companies by year (supplier figures)</summary>
      <p className="my-1">These raw counts precede chart-specific input and weight checks. Quarterly counts here group the year and do not prove coverage of an individual quarter.</p>
      {[...periods].sort(([a], [b]) => b.localeCompare(a)).map(([year, values]) => <div key={year} className="my-2"><strong>{year}</strong>
        <div className="grid grid-cols-2 gap-x-3">{Object.entries(values).map(([key, count]) => <div key={key}>{key.replaceAll('_', ' ')}: {count}</div>)}</div></div>)}
    </details>
    <details><summary className="cursor-pointer">Request plan and selection reasons</summary>
      <p className="my-1">Only eligible companies with verified missing core statements or estimates are requested. Feeds checked within 24 hours are skipped. Optional fields alone do not trigger a statement download.</p>
      {planned.selection?.map(c => <div key={c.company_id} className="my-2 border-t border-neutral-800 pt-1">
        <strong>{c.company ?? c.company_id}</strong> · {c.exchange}:{c.ticker} · ID {c.company_id}
        <div>Missing: {c.missing_metrics.join(', ')}. Statements: {c.statements}. Estimates: {c.estimates}.</div>
      </div>)}
    </details>
    <details><summary className="cursor-pointer">Every company included in the stored-data check ({audit.companies})</summary>
      {audit.companies_checked?.map(c => <div key={c.company_id} className="my-1">{c.company ?? c.company_id} · {c.exchange}:{c.ticker} · ID {c.company_id}</div>)}
    </details>
    <details><summary className="cursor-pointer">Skipped companies and subscription restrictions ({planned.unavailable.length})</summary>
      {planned.unavailable.map(c => <div key={c.company_id} className="my-1">{c.company ?? c.company_id}: {c.reason}</div>)}
    </details>
    {receipts.map((r, i) => <details key={i} open><summary className="cursor-pointer">{r.feed}: {r.receipt.attempted ?? 0} attempted · {r.receipt.failed ?? 0} unavailable · {r.receipt.deferred ?? 0} deferred · {r.receipt.calls ?? 0} calls</summary>
      <p>Checked successfully: {r.receipt.loaded ?? 0}. Stored data points changed: {r.receipt.rows ?? 0}.</p>
      <details><summary className="cursor-pointer">Every company response</summary>{r.receipt.outcomes?.map(c => <div key={c.company_id} className="my-2 border-t border-neutral-800 pt-1">
        <strong>{c.company}</strong> · {c.exchange}:{c.ticker} · ID {c.company_id}
        <div>{c.reason || c.status} · {c.rows} changed rows · {c.calls} calls</div>
      </div>)}</details>
    </details>)}
    <p className="text-fg-muted">The timeline below contains live company progress, raw diagnostics and timings for every stage. Chart reads continue while missing data downloads in the background.</p>
  </section>;
}

export default function JobDetails({ job }: { job: JobToast }) {
  const [events, setEvents] = useState<Event[]>([]);
  const [error, setError] = useState('');
  useEffect(() => {
    if (job.id.startsWith('local:')) return;
    setEvents([]);
    setError('');
    let alive = true;
    let cursor = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const ctrl = new AbortController();
    const load = async () => {
      try {
        let more = true;
        while (alive && more) {
          const response = await apiFetch(`${API_URL}/api/jobs/${encodeURIComponent(job.id)}/details?after=${cursor}`,
            { signal: ctrl.signal });
          if (!response.ok) throw new Error('Detailed log is no longer available.');
          const data = await response.json() as { events: Event[]; next_cursor: number; has_more: boolean };
          if (!alive) return;
          setEvents((current) => [...current, ...data.events]);
          cursor = data.next_cursor;
          more = data.has_more;
        }
        if (alive && job.status === 'running') timer = setTimeout(() => { void load(); }, 1500);
      } catch (reason) {
        if (alive) setError(reason instanceof Error ? reason.message : String(reason));
      }
    };
    void load();
    return () => { alive = false; ctrl.abort(); if (timer) clearTimeout(timer); };
  }, [job.id, job.status]);
  const items: Event[] = events.length ? events : (job.details ?? []).map((message, seq) => ({ seq, kind: 'progress', message }));
  return <div className="mt-2 max-h-[70vh] overflow-auto rounded-lg border border-neutral-800/60 bg-page/50 p-3 text-[11px] text-fg-muted">
    <div className="mb-2 font-mono whitespace-pre-wrap break-words">{job.kind} · {job.status}{job.summary ? `\n${job.summary}` : ''}</div>
    {error && <p>{error}</p>}
    {job.kind === 'fundamentals.benchmark-repair' && <BenchmarkOverview events={items} />}
    <ol role="log" aria-live="polite" className="space-y-2">
      {items.map((event) => {
        const { seq, kind, message, elapsed_ms, ...technical } = event;
        return <li key={seq}>
          <div className="whitespace-pre-wrap break-words">
            {elapsed_ms != null ? `${(elapsed_ms / 1000).toFixed(2)}s · ` : ''}{kind}: {message}
          </div>
          {Object.keys(technical).length > 0 && <details><summary className="cursor-pointer">Raw details{event.profile ? ' and profiling' : ''}</summary><pre className="mt-1 whitespace-pre-wrap break-words font-mono text-[10px]">{JSON.stringify(technical, null, 2)}</pre></details>}
        </li>;
      })}
    </ol>
    {!events.length && !job.details?.length && !error && <p>Waiting for diagnostic events…</p>}
  </div>;
}
