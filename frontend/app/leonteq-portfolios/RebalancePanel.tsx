'use client';

import { useEffect, useRef, useState } from 'react';
import { API_URL } from '../../lib/apiUrl';
import { apiFetch } from '../../lib/apiFetch';
import type { PortfolioConnection } from './ConnectionHoldingsView';
import type { CompositionResult } from './CompositionTable';
import type { AirsComposition } from './AirsCompositionView';
import { holdingKey, type HoldingRule } from './matchHoldings';
import { rebalancePlan, type RebalanceAction } from './rebalancePlan';
import { refreshRequest } from './refreshProgress';
import RecentTransactionsPanel from './RecentTransactionsPanel';

type Result = { action: RebalanceAction; success: boolean; message?: string; reverted?: boolean; revertError?: string; revertUncertain?: boolean };
const button = 'rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-sm disabled:opacity-50';
function formatSnapshotDate(value?: string | null, withTime = true) {
  if (!value) return 'Date unavailable';
  const date = new Date(value.length === 10 ? `${value}T12:00:00+01:00` : value);
  if (Number.isNaN(date.getTime())) return 'Date unavailable';
  const parts = new Intl.DateTimeFormat('nl-NL', {
    timeZone: 'Europe/Amsterdam', day: 'numeric', month: 'short', year: 'numeric',
    ...(withTime ? { hour: '2-digit', minute: '2-digit', hourCycle: 'h23' as const } : {}),
  }).formatToParts(date);
  const part = (type: Intl.DateTimeFormatPartTypes) => parts.find((item) => item.type === type)?.value ?? '';
  return `${part('day')} ${part('month').replace(/\.$/, '')} ${part('year')}${withTime ? ` ${part('hour')}:${part('minute')}` : ''}`;
}
async function json<T>(url: string, options = {}) {
  const body = (options as RequestInit).body;
  const refresh = url.includes('refresh=true') || (typeof body === 'string' && JSON.parse(body).refresh === true);
  const response = refresh
    ? await refreshRequest(`${API_URL}${url}`, options, 'Rebalance · Refresh portfolio holdings')
    : await apiFetch(`${API_URL}${url}`, options);
  if (!response.ok) {
    const failure = await response.json().catch(() => null) as { detail?: unknown } | null;
    throw new Error(typeof failure?.detail === 'string' ? failure.detail : 'Could not load saved data. Select Refresh compositions.');
  }
  return await response.json() as T;
}
function FullPortfolioRebalance({ active = true, onBusyChange }: { active?: boolean; onBusyChange?: (busy: boolean) => void }) {
  const [links, setLinks] = useState<PortfolioConnection[]>([]);
  const [selected, setSelected] = useState('');
  const [dummy, setDummy] = useState(true);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [elapsed, setElapsed] = useState(0);
  const [activeAction, setActiveAction] = useState<string | null>(null);
  const [singleRun, setSingleRun] = useState(false);
  const [stopped, setStopped] = useState(false);
  const [reverting, setReverting] = useState<string | null>(null);
  const execution = useRef<AbortController | null>(null);
  const [actions, setActions] = useState<RebalanceAction[]>([]);
  const [results, setResults] = useState<Result[]>([]);
  const [issues, setIssues] = useState<string[]>([]);
  const [prepared, setPrepared] = useState(false);
  const [snapshot, setSnapshot] = useState<AirsComposition | null>(null);
  const [sourceTimestamp, setSourceTimestamp] = useState<string | undefined>();
  const [status, setStatus] = useState('');
  const [maxChanges, setMaxChanges] = useState('');
  const validLimit = maxChanges === '' || (/^[1-9]\d*$/.test(maxChanges) && Number.isSafeInteger(Number(maxChanges)));
  const alive = useRef(true), busy = useRef(false);
  useEffect(() => { onBusyChange?.(running || reverting !== null); }, [running, reverting, onBusyChange]);
  const selection = useRef({ links, selected });
  selection.current = { links, selected };
  useEffect(() => {
    if (!running) return;
    const started = Date.now();
    setElapsed(0);
    const timer = setInterval(() => setElapsed(Math.floor((Date.now() - started) / 1000)), 1000);
    return () => clearInterval(timer);
  }, [running]);
  useEffect(() => {
    alive.current = true;
    return () => { alive.current = false; execution.current?.abort(); };
  }, []);
  useEffect(() => {
    if (!active || busy.current) return;
    let current = true;
    setLoading(true);
    void json<{ links: PortfolioConnection[] }>('/api/leonteq/portfolio-links?kind=fixed', { cache: 'no-store', noReadCache: true })
      .then((data) => {
        if (!current || !alive.current) return;
        const previous = selection.current.links[Number(selection.current.selected)];
        const index = previous ? data.links.findIndex((link) => link.leonteq_name === previous.leonteq_name && link.airs_portfolio_id === previous.airs_portfolio_id) : -1;
        setLinks(data.links);
        setSelected(index >= 0 ? String(index) : data.links.length ? '0' : '');
        if (previous && index < 0) { setPrepared(false); setActions([]); setResults([]); setSnapshot(null); setStatus(''); setStopped(false); }
      })
      .catch((error: Error) => { if (current && alive.current) setIssues([error.message]); })
      .finally(() => { if (current && alive.current) setLoading(false); });
    return () => { current = false; };
  }, [active]);
  function reset() { setPrepared(false); setActions([]); setResults([]); setIssues([]); setStatus(''); setStopped(false); }
  function stop() {
    execution.current?.abort();
    setStopped(true);
    if (dummy) setRunning(false);
    setStatus(dummy ? 'Stopped by you. No AIRS changes.' : 'Stopping. Verifying submitted write…');
  }
  async function saveWeight(action: RebalanceAction, expected: number, percentage: number, source = snapshot) {
    const row = source?.rows.find((item) => holdingKey({ isin: item.ISINCode, name: item.Fonds, weight: null }) === action.id);
    if (!source?.datum || !row?.id) throw new Error('Missing AIRS row identity.');
    const response = await apiFetch(`${API_URL}/api/leonteq/airs/model-portfolios/${links[Number(selected)].airs_portfolio_id}/positions/${row.id}/percentage`, {
      method: 'PATCH', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ datum: source.datum, expected_percentage: expected, percentage }),
    });
    if (!response.ok) {
      const failure = await response.json().catch(() => null);
      if (failure?.detail === 'percentage_changed' || failure?.detail === 'row_changed') throw new Error('This holding changed in AIRS. Refresh compositions before retrying; no additional change was made.');
      throw new Error('Save failed or unverified. Refresh before retrying.');
    }
    const verified = await response.json() as AirsComposition;
    const saved = verified.rows.filter((item) => item.id === row.id);
    if (verified.datum !== source.datum || saved.length !== 1 || saved[0].Percentage !== percentage || saved[0].Fonds !== row.Fonds || saved[0].ISINCode !== row.ISINCode) throw new Error('Save outcome uncertain. Refresh before retrying.');
    return verified;
  }
  async function revert(action: RebalanceAction) {
    const result = results.find((item) => item.action.id === action.id);
    if (busy.current || !result?.success || result.reverted || result.revertUncertain) return;
    busy.current = true; setReverting(action.id); setLoading(true);
    setStatus(dummy ? `Reverting simulated change for ${action.name}…` : `Restoring ${action.name} to ${action.current}% in AIRS…`);
    try {
      const verified = !dummy ? await saveWeight(action, action.target, action.current) : null;
      if (alive.current) {
        setResults((current) => current.map((item) => item.action.id === action.id ? { ...item, reverted: true, revertError: undefined } : item));
        setStatus(dummy ? `Simulated change reverted for ${action.name}. No AIRS weights were changed.` : verified?.already_at_target
          ? `${action.name} was already restored to ${action.current}% in AIRS. Verified; no additional change was made.`
          : `${action.name} restored to ${action.current}% and verified in AIRS.`);
      }
    } catch (error) {
      if (alive.current) {
        const message = error instanceof Error ? error.message : 'Revert outcome uncertain. Refresh before retrying.';
        setResults((current) => current.map((item) => item.action.id === action.id ? { ...item, revertError: message, revertUncertain: true } : item));
        setStatus(`Revert failed for ${action.name}. ${message}`);
      }
    } finally { busy.current = false; if (alive.current) { setReverting(null); setLoading(false); } }
  }
  async function prepare(refresh = false) {
    if (busy.current || selected === '') return;
    busy.current = true; reset(); setLoading(true);
    const link = links[Number(selected)];
    try {
      const params = new URLSearchParams({ leonteq_name: link.leonteq_name, airs_portfolio_id: String(link.airs_portfolio_id) });
      const [left, savedRight, rules] = await Promise.all([
        json<CompositionResult>('/api/leonteq/portfolios/composition', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: link.leonteq_name, refresh, saved_only: !refresh }) }),
        json<AirsComposition>(`/api/leonteq/airs/model-portfolios/${link.airs_portfolio_id}/composition${refresh ? '?refresh=true' : '?saved_only=true'}`, { cache: 'no-store', noReadCache: true }),
        json<{ rules: HoldingRule[] }>(`/api/leonteq/portfolio-links/holdings?${params}`, { cache: 'no-store', noReadCache: true }),
      ]);
      const right = savedRight;
      if (left.status === 'not_cached') throw new Error('No saved Leonteq holdings. Select Refresh compositions.');
      if (!left.success || !left.groups || right.no_fixed_model || !Array.isArray(rules.rules)) throw new Error('A complete fixed portfolio composition is required.');
      if (left.refresh_error) throw new Error(`Leonteq refresh failed (${left.refresh_error}).`);
      const plan = rebalancePlan(left.groups.flatMap((group) => group.positions), right.rows.map((row) => ({ name: row.Fonds, isin: row.ISINCode, weight: row.Percentage == null ? null : String(row.Percentage) })), rules.rules);
      for (const action of plan.actions) {
        if (Number(action.target.toFixed(6)) !== action.target) plan.issues.push(`Weight exceeds AIRS precision: ${action.name}`);
      }
      if (alive.current) { setStatus(''); setSourceTimestamp(left.retrieved_at); setSnapshot(right); setActions(plan.actions); setIssues(plan.issues); setPrepared(true); }
    } catch (error) { if (alive.current) { setStatus(''); setIssues([error instanceof Error ? error.message : 'Could not prepare actions.']); } }
    finally { busy.current = false; if (alive.current) setLoading(false); }
  }
  async function start(onlyId?: string) {
    const work = actions.filter((action) => (!onlyId || action.id === onlyId) && !results.some((result) => result.action.id === action.id));
    if (busy.current || stopped || !prepared || issues.length || results.some((result) => !result.success || result.revertUncertain) || !work.length || !validLimit) return;
    const controller = new AbortController();
    execution.current = controller;
    setSingleRun(Boolean(onlyId));
    busy.current = true; setRunning(true); setStatus(dummy ? 'Simulating actions…' : 'Updating AIRS weights…');
    const limit = maxChanges === '' ? Infinity : Number(maxChanges);
    let successes = 0;
    try {
      let liveSnapshot = snapshot;
      if (!dummy) {
        if (!liveSnapshot || liveSnapshot.cache_upgrade_available || !liveSnapshot.datum || liveSnapshot.rows.some((row) => !row.id)) {
          setStatus('Checking editable AIRS composition…');
          liveSnapshot = await json<AirsComposition>(`/api/leonteq/airs/model-portfolios/${links[Number(selected)].airs_portfolio_id}/composition?refresh=true`, { cache: 'no-store', noReadCache: true });
          if (!alive.current || controller.signal.aborted) return;
          if (snapshot?.datum && liveSnapshot.datum !== snapshot.datum) throw new Error('AIRS composition date changed. Prepare a new action list.');
          setSnapshot(liveSnapshot);
        }
        if (!liveSnapshot.datum || liveSnapshot.cache_upgrade_available) throw new Error('AIRS edit metadata is unavailable. Refresh compositions.');
        for (const action of work) {
          const matches = liveSnapshot.rows.filter((row) => holdingKey({ isin: row.ISINCode, name: row.Fonds, weight: null }) === action.id);
          if (matches.length !== 1 || !matches[0].id || matches[0].Percentage !== action.current) throw new Error(`AIRS holding changed: ${action.name}. Prepare a new action list.`);
        }
      }
      for (let index = 0; index < work.length; index++) {
        if (controller.signal.aborted || !alive.current) return;
        setActiveAction(`${index + 1}/${work.length} · ${work[index].name} · ${work[index].current}% → ${work[index].target}%`);
        if (dummy) await new Promise<void>((resolve) => {
          const finish = () => { clearTimeout(timer); controller.signal.removeEventListener('abort', finish); resolve(); };
          const timer = setTimeout(finish, 400);
          controller.signal.addEventListener('abort', finish, { once: true });
          if (controller.signal.aborted) finish();
        });
        if (!alive.current || controller.signal.aborted) return;
        let success = dummy;
        let message: string | undefined;
        if (!dummy) {
          try {
            // Never abort or retry a submitted write: its outcome must be verified.
            await saveWeight(work[index], work[index].current, work[index].target, liveSnapshot);
            success = true;
          } catch (error) { message = error instanceof Error ? error.message : 'Save outcome uncertain. Refresh before retrying.'; }
        }
        if (!alive.current) return;
        setResults((current) => [{ action: work[index], success, message }, ...current]);
        if (!success) { setStatus(`Stopped at action ${index + 1}: ${dummy ? 'simulated failure' : message}.`); return; }
        if (controller.signal.aborted) { setStatus('Stopped. Submitted write verified; saved changes remain.'); return; }
        successes++;
        if (successes >= limit && index + 1 < work.length) {
          setStatus(`Stopped automatically after ${successes} successful ${dummy ? 'simulated ' : ''}change${successes === 1 ? '' : 's'}: maximum reached.`);
          return;
        }
      }
      setStatus(dummy ? 'Dummy complete. No AIRS changes.' : 'Complete. AIRS changes verified.');
    } catch (cause) { if (alive.current) { setStopped(true); setStatus(cause instanceof Error ? cause.message : 'Live checks failed. No further writes started.'); } }
    finally { execution.current = null; busy.current = false; if (alive.current) { setRunning(false); setActiveAction(null); } }
  }
  const completed = new Set(results.map((result) => result.action.id));
  const executionBlocked = stopped || results.some((result) => !result.success || result.revertUncertain);
  const rows = [...results.map((result) => ({ ...result.action, status: result.reverted ? (dummy ? 'Simulated revert' : 'Reverted') : `${dummy ? 'Simulated ' : ''}${result.success ? 'success' : 'failure'}`, message: result.revertError ?? result.message, canRevert: result.success && !result.reverted && !result.revertUncertain })), ...actions.filter((action) => !completed.has(action.id)).map((action) => ({ ...action, status: executionBlocked || (results.length && !running && !singleRun) ? 'Not executed' : 'Proposed', message: undefined, canRevert: false }))];
  return <section className="space-y-4" aria-label="Rebalance">
    <h2 className="text-lg font-semibold">Rebalance</h2>
    <div className="space-y-4 rounded-xl border border-neutral-800/40 bg-elevated/40 p-5">
      <div className="grid items-end gap-4 lg:grid-cols-[minmax(0,1fr)_auto]">
      <label className="grid min-w-0 gap-2 text-xs font-medium text-fg-muted">Portfolio connection<select className="h-11 w-full min-w-0 rounded-lg border border-neutral-800/40 bg-elevated px-3 text-sm font-medium text-fg focus:outline-none focus:ring-2 focus:ring-accent-600 disabled:opacity-50" value={selected} disabled={loading || running} onChange={(event) => { setSelected(event.target.value); reset(); }}>
        {!links.length && <option value="">No saved connections</option>}
        {links.map((link, index) => <option key={`${link.leonteq_name}-${link.airs_portfolio_id}`} value={index}>{link.leonteq_name} → {link.airs_name ?? link.airs_portfolio_id}</option>)}
      </select></label>
      <label className="flex h-11 cursor-pointer items-center justify-between gap-6 rounded-lg border border-neutral-800/40 bg-elevated px-3 text-sm font-medium">
        <span>Dummy mode</span>
        <span className="relative inline-flex shrink-0">
          <input type="checkbox" className="peer sr-only" checked={dummy} disabled={loading || running || results.length > 0} onChange={(event) => setDummy(event.target.checked)} />
          <span aria-hidden="true" className="h-6 w-11 rounded-full bg-neutral-700 transition-colors peer-checked:bg-accent-600 peer-focus-visible:ring-2 peer-focus-visible:ring-accent-600 peer-focus-visible:ring-offset-2 peer-disabled:opacity-50" />
          <span aria-hidden="true" className="absolute left-1 top-1 h-4 w-4 rounded-full bg-white shadow-sm transition-transform peer-checked:translate-x-5" />
        </span>
      </label>
      </div>
      <div className="flex flex-col gap-3 border-t border-neutral-800/30 pt-4 sm:flex-row sm:items-center sm:justify-between">
        <p className="text-xs text-fg-muted">{dummy ? 'Dummy · No AIRS changes' : 'Live · Writes to AIRS'}</p>
        <div className="grid grid-cols-1 gap-2 sm:flex sm:shrink-0">
          <button className="h-10 rounded-lg bg-accent-600 px-4 text-sm font-medium text-white transition-colors hover:bg-accent-600/80 disabled:opacity-50" disabled={loading || running || selected === ''} onClick={() => void prepare()}>{loading ? 'Loading…' : 'Prepare action list'}</button>
          <button className={`${button} h-10 transition-colors hover:bg-overlay/5`} disabled={loading || running || selected === ''} onClick={() => void prepare(true)}>Refresh compositions</button>
        </div>
      </div>
    </div>
    {issues.map((issue, index) => <p role="alert" key={index} className="text-sm text-neg-300">{issue}</p>)}
    {prepared && <>
      <dl className="grid grid-cols-1 gap-4 rounded-xl border border-neutral-800/40 bg-elevated/40 p-4 sm:grid-cols-3">
        {[
          ['Leonteq snapshot', formatSnapshotDate(sourceTimestamp)],
          ['AIRS composition date', formatSnapshotDate(snapshot?.datum, false)],
          ['AIRS retrieved', formatSnapshotDate(snapshot?.cached_at ?? snapshot?.retrieved_at)],
        ].map(([label, value]) => <div key={label}>
          <dt className="text-xs text-fg-muted">{label}</dt>
          <dd className="mt-1 text-sm font-medium tabular-nums">{value}</dd>
        </div>)}
      </dl>
      <div className="grid grid-cols-1 items-end gap-4 md:grid-cols-2">
        <label className="grid min-w-0 gap-2 text-sm">Maximum successful changes<input type="number" min="1" step="1" placeholder="Unlimited" aria-describedby="rebalance-limit-help" value={maxChanges} disabled={running || results.length > 0} onChange={(event) => setMaxChanges(event.target.value)} className={`${button} h-10 w-full min-w-0`} /></label>
        <div className="flex gap-2">
          <button className="h-10 flex-1 rounded-lg border border-accent-600 bg-accent-600 px-3 text-sm font-medium text-white disabled:cursor-not-allowed disabled:border-neutral-700 disabled:bg-neutral-800 disabled:text-neutral-300" disabled={running || stopped || !!issues.length || !actions.length || !!results.length || !validLimit} onClick={() => void start()}>{running ? 'Running…' : stopped ? 'Run stopped' : results.length ? 'Run finished' : dummy ? 'Start dummy rebalance' : 'Start live rebalance'}</button>
          <button type="button" className="h-10 rounded-lg border border-red-700 bg-red-700 px-4 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-50" disabled={!running} onClick={stop}>Stop</button>
        </div>
      </div>
      <p id="rebalance-limit-help" className="text-xs text-fg-muted">Empty = unlimited · 1 = stop after one success</p>
      {!validLimit && <p role="alert" className="text-sm text-neg-300">Enter a positive whole number, or leave empty.</p>}
      <p role="status" aria-live="polite" className="text-sm">{status || (actions.length ? `${actions.length} changes · Stop on first failure` : 'No changes needed.')}</p>
      {running && <p className="text-sm text-fg-muted">{activeAction} · {elapsed}s{elapsed >= 20 ? ' · Waiting for AIRS; save verification pending.' : ''}</p>}
      <div className="overflow-x-auto rounded-xl border border-neutral-800/40"><table className="w-full text-sm">
        <caption className="sr-only">Rebalance actions</caption>
        <thead className="bg-elevated"><tr>{['Status', 'AIRS holding', 'ISIN / connection', 'Previous AIRS value', 'New value (Leonteq)', 'Change (pp)', 'Action'].map((title) => <th key={title} className="px-4 py-3 text-left font-medium">{title}</th>)}</tr></thead>
        <tbody className="divide-y divide-neutral-800/30">{rows.map((row) => <tr key={row.id} className={row.status.endsWith('failure') ? 'text-neg-300' : row.status.endsWith('success') ? 'text-pos-300' : ''}>
          <td className="px-4 py-3">{row.status}{row.message && <p className="mt-1 text-xs text-neg-300">{row.message}</p>}</td><td className="px-4 py-3">{row.name}</td><td className="px-4 py-3">{row.isin}</td><td className="px-4 py-3">{row.current}%</td><td className="px-4 py-3">{row.target}%</td><td className="px-4 py-3">{row.target > row.current ? '+' : ''}{Number((row.target - row.current).toFixed(6))} pp</td>
          <td className="px-4 py-3">{!completed.has(row.id) && <button type="button" className={button} disabled={running || loading || reverting !== null || executionBlocked || !!issues.length || !validLimit} aria-label={`${dummy ? 'Simulate' : 'Execute'} ${row.name}`} onClick={() => void start(row.id)}>{dummy ? 'Simulate' : 'Execute'}</button>}{row.canRevert && <button type="button" className={button} disabled={running || loading || reverting !== null} aria-label={`Revert ${row.name} to ${row.current}%`} onClick={() => void revert(row)}>{reverting === row.id ? 'Reverting…' : 'Revert'}</button>}</td>
        </tr>)}</tbody>
      </table></div>
    </>}
  </section>;
}

export default function RebalancePanel({ active = true }: { active?: boolean }) {
  const [tab, setTab] = useState<'full' | 'recent'>('full');
  const [recentOpened, setRecentOpened] = useState(false);
  const [fullBusy, setFullBusy] = useState(false), [recentBusy, setRecentBusy] = useState(false);
  return <div className="space-y-4">
    <div role="tablist" aria-label="Rebalance view" className="inline-flex max-w-full overflow-x-auto rounded-lg border border-neutral-800/40 text-xs">
      {(['full', 'recent'] as const).map(value => <button key={value} id={`rebalance-tab-${value}`} type="button" role="tab" aria-selected={tab === value} aria-controls={`rebalance-panel-${value}`}
        disabled={fullBusy || recentBusy} className={`shrink-0 px-3 py-1.5 font-medium transition-colors disabled:opacity-50 ${tab === value ? 'bg-accent-600 text-white' : 'text-fg-subtle hover:bg-overlay/5'}`}
        onClick={() => { setTab(value); if (value === 'recent') setRecentOpened(true); }}>{value === 'full' ? 'Full portfolio' : 'Recent transactions'}</button>)}
    </div>
    <div role="tabpanel" id="rebalance-panel-full" aria-labelledby="rebalance-tab-full" hidden={tab !== 'full'}><FullPortfolioRebalance active={active && tab === 'full'} onBusyChange={setFullBusy} /></div>
    <div role="tabpanel" id="rebalance-panel-recent" aria-labelledby="rebalance-tab-recent" hidden={tab !== 'recent'}>{recentOpened && <RecentTransactionsPanel active={active && tab === 'recent'} onBusyChange={setRecentBusy} />}</div>
  </div>;
}
