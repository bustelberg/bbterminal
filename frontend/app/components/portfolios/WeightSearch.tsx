'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { exportToXlsx } from '../../../lib/tableExport';
import { sharpeExportColumns, sharpeExportRows } from './sharpeExport';

type Metrics = { sharpe?: number | null; sortino: number | null; annual_return_pct: number | null; max_drawdown_pct: number | null; weeks: number };
type Candidate = { weights: Record<string, number>; training: Metrics; validation: Metrics; full_history: Metrics };
type Bound = { variant: string; bucket: string; min_pct: number | null; default_pct: number | null; max_pct: number | null };
type Policy = { profiles: string[]; profile: string | null; all_bounds: Bound[] };
type InstrumentBlock = { bucket: string; average_sharpe?: number | null; period?: { start: string; end: string } | null; training: Metrics | null; validation: Metrics | null; instruments: Array<{
  name: string; training_weight_pct: number; test_weight_pct: number; training: Metrics | null; validation: Metrics | null;
  overall?: (Metrics & { start: string; end: string }) | null;
}> };
type SearchResult = { available: boolean; reason?: string; issues?: string[]; candidates?: Candidate[]; current?: Candidate;
  objective?: 'sharpe' | 'sortino';
  instrument_breakdown?: InstrumentBlock[];
  training_period?: { start: string; end: string } | null; test_period?: { start: string; end: string } | null;
  candidates_tested?: number; training_weeks?: number; validation_weeks?: number; from?: string; through?: string;
  method?: string; limitations?: string[]; annual_target_pct?: number; annual_cash_pct?: number;
  statistical_check?: { supported: boolean; reason: string; method?: string };
  proxy_history?: boolean; skipped_weeks?: string[]; coverage?: Array<{ bucket: string; coverage_pct: number | null; minimum_coverage_pct?: number | null; included: string[]; excluded: string[] }> };
const control = 'rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-xs disabled:opacity-50';
const number = (value: number | null | undefined, suffix = '') => value == null ? '—' : `${value.toFixed(2)}${suffix}`;
const buckets = ['Equity', 'Bonds', 'Alternatives', 'Cash'];
const dateLabel = (value: string) => new Date(`${value}T12:00:00Z`).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' }).toLowerCase();

export default function WeightSearch({ portefeuille, startDate, disabled }: { portefeuille: string; startDate: string; disabled: boolean }) {
  const [busy, setBusy] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [target, setTarget] = useState('2');
  const [cash, setCash] = useState('0');
  const [availableHistory, setAvailableHistory] = useState(true);
  const [objective, setObjective] = useState<'sharpe' | 'sortino'>('sharpe');
  const [policy, setPolicy] = useState<Policy | null>(null);
  const [profile, setProfile] = useState('');
  const [policyError, setPolicyError] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    void apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/weight-search/bounds`).then(async response => {
      const payload = await response.json();
      if (!response.ok) throw new Error('Could not load profile bounds.');
      if (!cancelled) { setPolicy(payload); setProfile(payload.profile ?? ''); }
    }).catch(() => { if (!cancelled) setPolicyError('Could not load profile bounds. Reopen Weights to retry.'); });
    return () => { cancelled = true; };
  }, [portefeuille]);
  const [result, setResult] = useState<SearchResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [searched, setSearched] = useState('');
  const identity = JSON.stringify([portefeuille, startDate, target, cash, availableHistory, profile, objective]);
  const bounds = policy?.all_bounds?.filter(row => row.variant === profile) ?? [];
  const completeBounds = buckets.every(bucket => bounds.some(row => row.bucket === bucket && row.min_pct != null && row.max_pct != null));
  const valid = target !== '' && cash !== '' && [Number(target), Number(cash)].every(value => Number.isFinite(value) && value >= 0 && value <= 20);
  async function find() {
    if (busy || disabled || !valid || !profile || !completeBounds) return;
    setBusy(true); setError(null); setResult(null); setSearched(identity);
    try {
      const query = new URLSearchParams({ start_date: startDate, annual_target: target, annual_cash: cash, available_history: String(availableHistory), profile, objective });
      const response = await apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/weight-search?${query}`, { method: 'POST', noReadCache: true });
      const payload = await response.json();
      if (!response.ok) throw new Error(typeof payload.detail === 'string' ? payload.detail : 'Could not search allocation weights.');
      if (payload.all_bounds && payload.profiles) setPolicy({ profiles: payload.profiles, profile: payload.profile, all_bounds: payload.all_bounds });
      setResult(payload);
    } catch (cause) { setError(cause instanceof Error ? cause.message : 'Could not search allocation weights.'); }
    finally { setBusy(false); }
  }
  const showResults = result?.available && searched === identity;
  async function downloadExcel() {
    if (!showResults || !result?.instrument_breakdown?.length || downloading) return;
    setDownloading(true); setError(null);
    try {
      await exportToXlsx(sharpeExportRows(result.instrument_breakdown, dateLabel), sharpeExportColumns,
        `Sharpe ${portefeuille} ${profile}`);
    } catch { setError('Could not download Excel. Please try again.'); }
    finally { setDownloading(false); }
  }
  const score = result?.objective ?? objective;
  const scoreLabel = score === 'sharpe' ? 'Sharpe' : 'Sortino';
  const rows = showResults ? [
    ...(result.current ? [{ label: 'Current', candidate: result.current }] : []),
    ...(result.candidates ?? []).slice(0, 3).map((candidate, index) => ({ label: `Candidate ${index + 1}`, candidate })),
  ] : [];
  return <section aria-label="Allocation search" className="space-y-3 rounded-xl border border-neutral-800/40 bg-card p-4">
    <div className="flex flex-wrap items-center justify-between gap-3"><h3 className="font-semibold">Allocation search</h3>
      <div className="flex gap-2">{showResults && !!result.instrument_breakdown?.length && <button className={control} disabled={downloading || busy} onClick={() => void downloadExcel()}>{downloading ? 'Downloading…' : 'Download Excel'}</button>}
      <button className={control} disabled={busy || disabled || !valid || !profile || !completeBounds} onClick={() => void find()}>{busy ? 'Searching…' : 'Find optimal weights'}</button></div></div>
    <div className="flex flex-wrap items-center gap-3"><label className="text-xs">Profile <select aria-label="Risk profile" value={profile} disabled={busy || !policy} onChange={event => setProfile(event.target.value)} className={control}><option value="">Choose profile</option>{policy?.profiles.map(name => <option key={name}>{name}</option>)}</select></label>
      {bounds.map(row => <span key={row.bucket} className="text-xs text-fg-muted">{row.bucket === 'Equity' ? 'Stocks' : row.bucket} {number(row.min_pct, '%')}–{number(row.max_pct, '%')}</span>)}
    </div>
    {policyError && <p role="alert" className="text-xs text-neg-300">{policyError}</p>}
    <details className="text-xs text-fg-muted"><summary className="cursor-pointer">Settings</summary><div className="mt-2 flex flex-wrap gap-3">
      <label>Rank by <select aria-label="Search objective" value={objective} disabled={busy} className={control} onChange={event => setObjective(event.target.value as 'sharpe' | 'sortino')}><option value="sharpe">Sharpe</option><option value="sortino">Sortino</option></select></label>
      <label className="flex items-center gap-2"><input type="checkbox" checked={availableHistory} disabled={busy} onChange={event => setAvailableHistory(event.target.checked)} />Use available history as asset-class proxies</label>
      {objective === 'sortino' && <label>Annual minimum acceptable return (%)<input aria-label="Annual minimum acceptable return" className={`${control} ml-2 w-20`} type="number" min="0" max="20" step="0.1" value={target} disabled={busy} onChange={event => setTarget(event.target.value)} /></label>}
      <label>Cash return / Sharpe risk-free rate (% p.a.)<input aria-label="Assumed annual cash return" className={`${control} ml-2 w-20`} type="number" min="0" max="20" step="0.1" value={cash} disabled={busy} onChange={event => setCash(event.target.value)} /></label>
    </div><p className="mt-2">5% increments within profile bounds. Weekly rebalancing; income and costs excluded. {availableHistory ? 'Available instruments are reweighted within their class each week.' : 'Complete current holdings required.'} Older weeks rank candidates; newer weeks test them. Test scores never select weights. The top three candidates are shown regardless of statistical significance.</p></details>
    {busy && <p role="status" className="text-xs">Aligning saved history and comparing allocations…</p>}
    {error && <p role="alert" className="text-xs text-neg-300">{error}</p>}
    {result && searched !== identity && <p role="status" className="text-xs text-fg-muted">Inputs changed. Run the search again to update these results.</p>}
    {result && searched === identity && !result.available && <p role="status" className="text-sm">{result.reason ?? 'Could not calculate an allocation.'}</p>}
    {showResults && <>
      <p role="status" className="text-xs text-fg-muted">{result.training_weeks} training / {result.validation_weeks} test weeks · Ranked by training {scoreLabel}.</p>
      {(result.training_period || result.test_period) && <p className="text-xs text-fg-muted">{result.training_period && <>Train: {dateLabel(result.training_period.start)} – {dateLabel(result.training_period.end)}</>}{result.training_period && result.test_period && ' · '}{result.test_period && <>Test: {dateLabel(result.test_period.start)} – {dateLabel(result.test_period.end)}</>}</p>}
      {!!rows.length && <div className="overflow-x-auto"><table className="w-full text-xs"><caption className="sr-only">Allocation training and test comparison</caption>
        <thead><tr>{['Allocation', 'Stocks', 'Bonds', 'Alternatives', 'Cash', `Train ${scoreLabel}`, `Test ${scoreLabel}`].map(label => <th key={label} className="whitespace-nowrap px-3 py-2 text-left">{label}</th>)}</tr></thead>
        <tbody>{rows.map(({ label, candidate }) => <tr key={label} className="border-t border-neutral-800/30"><th className="px-3 py-2 text-left">{label}</th>{buckets.map(bucket => <td key={bucket} className="px-3 py-2 tabular-nums">{number(candidate.weights[bucket], '%')}</td>)}<td className="px-3 py-2">{number(candidate.training?.[score])}</td><td className="px-3 py-2">{number(candidate.validation?.[score])}</td></tr>)}</tbody>
      </table></div>}
      <p className="text-xs text-fg-muted">Exploratory price returns.{result.proxy_history ? ' Uses proxy baskets.' : ''} {result.statistical_check?.supported ? 'Held-out improvement supported.' : 'Statistical improvement not established.'}</p>
    </>}
    {showResults && <details className="space-y-3 text-xs text-fg-muted"><summary className="cursor-pointer">Details</summary>
      {result.instrument_breakdown?.length ? result.instrument_breakdown.map(block => <div key={block.bucket}>
        <h4 className="mb-1 font-semibold text-fg">{block.bucket === 'Equity' ? 'Stocks' : block.bucket}</h4>
        <table className="w-full text-xs"><caption className="sr-only">{block.bucket} instrument full-period Sharpe</caption>
          <thead><tr>{['Instrument', 'Sharpe', 'Start', 'End'].map(label => <th key={label} className="px-3 py-2 text-left">{label}</th>)}</tr></thead>
          <tbody>{block.instruments.map((instrument, index) => <tr key={`${instrument.name}-${index}`} className="border-t border-neutral-800/30">
            <th className="px-3 py-2 text-left font-normal">{instrument.name}</th><td className="px-3 py-2">{number(instrument.overall?.sharpe)}</td><td className="px-3 py-2">{instrument.overall ? dateLabel(instrument.overall.start) : '—'}</td><td className="px-3 py-2">{instrument.overall ? dateLabel(instrument.overall.end) : '—'}</td>
          </tr>)}<tr className="border-t border-neutral-800/40 font-semibold text-fg">
            <th className="px-3 py-2 text-left">Average instrument Sharpe</th><td className="px-3 py-2">{number(block.average_sharpe)}</td><td className="px-3 py-2">{block.period ? dateLabel(block.period.start) : '—'}</td><td className="px-3 py-2">{block.period ? dateLabel(block.period.end) : '—'}</td>
          </tr></tbody>
        </table>
      </div>) : <p>Run the search again to load instrument scores.</p>}
    </details>}
  </section>;
}
