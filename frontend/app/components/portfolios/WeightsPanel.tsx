'use client';

import { useContext, useEffect, useRef, useState } from 'react';
import { RiskBundleContext } from './riskBundleContext';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { startJob } from '../../../lib/stores/jobs';
import { invalidateReadCache } from '../../../lib/readCache';
import { visibleWeeklyReturns, type InstrumentOption, type WeeklyReturns } from './weeklyReturns';
import { ReturnChart } from './WeeklyReturnsPanel';
import PanelDialog from './PanelDialog';
import WeightSearch from './WeightSearch';

type Series = Pick<WeeklyReturns, 'selected' | 'periods' | 'missing_observations'> & Partial<Pick<WeeklyReturns, 'start_date' | 'ytd_return'>>;
type Bundle = Pick<WeeklyReturns, 'portefeuille' | 'start_date' | 'through' | 'options'> & { instruments: Record<string, Series> };
type RowState = { status: string; detail?: string };
const control = 'rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-xs disabled:opacity-50';
const date = (value?: string | null) => value ? new Date(`${value.slice(0, 10)}T12:00:00Z`).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' }).toLowerCase() : '—';

function SavedReturns({ instrument, data, onClose }: { instrument: InstrumentOption; data: WeeklyReturns | null; onClose: () => void }) {
  const [hovered, onHover] = useState<string | null>(null);
  const [window, onWindow] = useState<[string, string] | null>(null);
  const scale = Math.max(1, ...(data?.selected?.weeks ?? []).map(cell => Math.abs(cell.return_pct ?? 0)).filter(Number.isFinite)) * 1.1;
  return <PanelDialog onClose={onClose} labelledBy="saved-weekly-returns-title">
    <div className="flex h-full min-h-0 flex-col overflow-hidden rounded-xl border border-neutral-200 bg-white text-fg shadow-2xl">
    <div className="flex shrink-0 items-start justify-between gap-4 border-b border-neutral-200 p-4">
      <div><h3 id="saved-weekly-returns-title" className="font-semibold">{instrument.name}</h3><p className="mt-1 text-xs text-fg-muted">Stored weekly AIRS returns · {[instrument.isin, instrument.currency].filter(Boolean).join(' · ')}</p></div>
      <button className={control} onClick={onClose}>Close</button>
    </div>
    <div className="min-h-0 flex-1 overflow-auto p-4">
      {data ? <ReturnChart data={data} nl={false} scale={scale} linked={{ hovered, onHover, window, onWindow }} />
        : <p className="text-sm text-fg-muted">No weekly returns stored yet. Use Fetch to retrieve this instrument’s history.</p>}
      {window && <button className={control} onClick={() => onWindow(null)}>Clear selection</button>}
    </div>
    </div>
  </PanelDialog>;
}

export default function WeightsPanel({ portefeuille }: { portefeuille?: string | null }) {
  const risk = useContext(RiskBundleContext);
  const bundle = risk?.views['weekly-returns'] as Bundle | undefined;
  const [startDate, setStartDate] = useState('2025-02-01');
  const [states, setStates] = useState<Record<string, RowState>>({});
  const [fresh, setFresh] = useState<Record<string, WeeklyReturns>>({});
  const [opened, setOpened] = useState<InstrumentOption | null>(null);
  const [pending, setPending] = useState<Set<string>>(new Set());
  const [allStatus, setAllStatus] = useState<string | null>(null);
  const pendingRef = useRef(new Set<string>()), queue = useRef(Promise.resolve()), alive = useRef(true);
  useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);
  const today = new Intl.DateTimeFormat('sv-SE', { timeZone: 'Europe/Amsterdam' }).format(new Date());
  const validDate = /^\d{4}-\d{2}-\d{2}$/.test(startDate) && startDate >= '1900-01-01' && startDate <= today;
  function savedFor(id: string): WeeklyReturns | null {
    const series = bundle?.instruments[id];
    return fresh[id] ?? (bundle && series ? { ...bundle, ...series } : null);
  }

  async function fetchAll() {
    if (!portefeuille || !bundle?.options.length || !validDate || pendingRef.current.size) return;
    const ids = bundle.options.map(row => row.id);
    pendingRef.current = new Set(ids); setPending(new Set(ids));
    setStates(current => ({ ...current, ...Object.fromEntries(ids.map(id => [id, { status: 'Fetching…' }])) }));
    setAllStatus('Fetching all instruments…');
    try {
      const query = new URLSearchParams({ start_date: startDate });
      const { done } = await startJob(`${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/weekly-returns/all/job?${query}`,
        'All AIRS instrument histories', undefined, progress => {
          if (alive.current && progress.message) setAllStatus(progress.message);
        });
      const job = await done;
      invalidateReadCache('All AIRS instrument weekly histories updated');
      const response = await apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/weekly-returns/all`, { cache: 'no-store', noReadCache: true });
      const result = await response.json();
      if (!response.ok) throw new Error(typeof result.detail === 'string' ? result.detail : 'Could not read saved histories.');
      if (alive.current) {
        const saved = result as Bundle;
        setFresh(current => ({ ...current, ...Object.fromEntries(Object.entries(saved.instruments).map(([id, series]) => [id, { ...saved, ...series }])) }));
        const status = job.status === 'done' ? 'Updated' : job.status === 'cancelled' ? 'Cancelled' : 'Failed';
        setStates(current => ({ ...current, ...Object.fromEntries(ids.map(id => [id, { status }])) }));
        setAllStatus(job.status === 'done' ? 'All instrument histories updated from shared weekly snapshots.' : 'Saved progress retained. See the task details.');
        risk?.reload?.();
      }
    } catch (cause) {
      if (alive.current) {
        setAllStatus(cause instanceof Error ? cause.message : 'Could not fetch all histories.');
        setStates(current => ({ ...current, ...Object.fromEntries(ids.map(id => [id, { status: 'Failed', detail: 'Saved progress retained. Retry Fetch all.' }])) }));
      }
    } finally {
      pendingRef.current.clear();
      if (alive.current) setPending(new Set());
    }
  }

  function fetchRow(instrument: InstrumentOption) {
    if (!portefeuille || !validDate || pendingRef.current.has(instrument.id)) return;
    const earliest = startDate;
    pendingRef.current.add(instrument.id); setPending(new Set(pendingRef.current));
    setStates(current => ({ ...current, [instrument.id]: { status: 'Queued' } }));
    queue.current = queue.current.then(async () => {
      if (!alive.current) return;
      try {
        setStates(current => ({ ...current, [instrument.id]: { status: 'Fetching…' } }));
        const query = new URLSearchParams({ instrument_id: instrument.id, start_date: earliest });
        const { done } = await startJob(`${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/weekly-returns/job?${query}`,
          `${instrument.name} · AIRS weekly history`, undefined, progress => {
            if (alive.current) setStates(current => ({ ...current, [instrument.id]: { status: 'Fetching…', detail: progress.message } }));
          });
        const job = await done;
        invalidateReadCache('AIRS instrument weekly history updated');
        const response = await apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/weekly-returns?instrument_id=${encodeURIComponent(instrument.id)}`, { cache: 'no-store', noReadCache: true });
        const result = await response.json();
        if (!response.ok) throw new Error(typeof result.detail === 'string' ? result.detail : 'Could not read saved history.');
        if (alive.current) {
          setFresh(current => ({ ...current, [instrument.id]: result }));
          setStates(current => ({ ...current, [instrument.id]: { status: job.status === 'done' ? 'Updated' : job.status === 'cancelled' ? 'Cancelled' : 'Failed',
            detail: job.status === 'done' ? `Checked back to ${date(earliest)} or the history boundary.` : 'Saved progress retained. See the task details.' } }));
          risk?.reload?.();
        }
      } catch (cause) {
        if (alive.current) setStates(current => ({ ...current, [instrument.id]: { status: 'Failed', detail: cause instanceof Error ? cause.message : 'Could not fetch history.' } }));
      } finally {
        pendingRef.current.delete(instrument.id);
        if (alive.current) setPending(new Set(pendingRef.current));
      }
    });
  }

  if (!portefeuille) return <p className="text-sm text-fg-muted">Link an AIRS portfolio to see instruments.</p>;
  return <section aria-label="Weights" className="space-y-4">
    <WeightSearch key={portefeuille} portefeuille={portefeuille} startDate={startDate} disabled={!validDate || pending.size > 0} />
    <div className="flex flex-wrap items-end justify-between gap-3">
      <div><h3 className="font-semibold">Weights</h3><p className="mt-1 text-xs text-fg-muted">Weekly AIRS returns by instrument. Fetch from today backwards, stopping at the earliest date or the history boundary.</p></div>
      <label className="grid gap-1 text-xs text-fg-muted">Earliest date<input aria-label="Earliest date" type="date" value={startDate} min="1900-01-01" max={today} disabled={pending.size > 0} onChange={event => setStartDate(event.target.value)} className={control} /></label>
      <button className={control} disabled={!validDate || pending.size > 0 || !bundle?.options.length} onClick={() => void fetchAll()}>Fetch all</button>
    </div>
    {allStatus && <p role="status" className="text-xs text-fg-muted">{allStatus}</p>}
    {!validDate && <p role="alert" className="text-xs text-neg-300">Choose an earliest date between 1 January 1900 and today.</p>}
    {!bundle && risk?.error && <p role="alert" className="text-xs text-neg-300">{risk.error}</p>}
    {!bundle && !risk?.error && <p role="status" className="text-sm text-fg-muted">Loading AIRS instruments…</p>}
    {bundle && <div className="overflow-x-auto rounded-xl border border-neutral-800/40"><table className="w-full text-xs">
      <caption className="sr-only">AIRS instruments and saved weekly returns</caption>
      <thead className="bg-elevated text-fg-muted"><tr>{['Instrument', 'ISIN', 'Currency', 'Return weeks', 'From', 'Through', 'Status', 'Action'].map(label => <th key={label} scope="col" className="px-3 py-3 text-left font-medium">{label}</th>)}</tr></thead>
      <tbody>{bundle.options.map(instrument => {
        const saved = savedFor(instrument.id);
        const visible = saved ? visibleWeeklyReturns(saved) : null;
        const row = states[instrument.id];
        return <tr key={instrument.id} className="border-t border-neutral-800/30">
          <th scope="row" className="px-3 py-3 text-left font-medium">{instrument.name}</th><td className="px-3 py-3 font-mono">{instrument.isin ?? '—'}</td><td className="px-3 py-3">{instrument.currency}</td>
          <td className="px-3 py-3 tabular-nums">{visible?.periods.length ?? 0}</td><td className="whitespace-nowrap px-3 py-3">{visible?.periods.length ? date(visible.start_date) : '—'}</td><td className="whitespace-nowrap px-3 py-3">{visible?.periods.length ? date(visible.through) : '—'}</td>
          <td className="max-w-64 px-3 py-3"><span>{row?.status ?? 'Saved history'}</span>{row?.detail && <p className="mt-1 text-fg-muted">{row.detail}</p>}</td>
          <td className="px-3 py-3"><div className="flex gap-2"><button className={control} disabled={!validDate || pending.has(instrument.id)} aria-label={`Fetch ${instrument.name}`} onClick={() => fetchRow(instrument)}>{pending.has(instrument.id) ? row?.status ?? 'Queued' : 'Fetch'}</button><button className={control} aria-label={`Open ${instrument.name}`} onClick={() => setOpened(instrument)}>Open</button></div></td>
        </tr>;
      })}</tbody>
    </table>{!bundle.options.length && <p className="p-4 text-sm text-fg-muted">No instruments found in the saved AIRS history.</p>}</div>}
    {opened && <SavedReturns key={opened.id} instrument={opened} data={savedFor(opened.id)} onClose={() => setOpened(null)} />}
  </section>;
}
