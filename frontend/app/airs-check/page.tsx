'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { useLang } from '../../lib/i18n';
import { startJob } from '../../lib/stores/jobs';
import type { AirsPortfolioOverview } from '../../lib/types/api';
import { useMgmtCopy } from '../components/management/managementCopy';
import { PriceCheckDetails, PriceCheckNotice, usePriceCheckCopy } from '../components/management/PriceCheckPanel';
import { canAnalyseAccount } from '../components/management/portfolioAccounts';
import { isExtremeDifference } from '../components/management/priceCheck';
import { usePriceChecks } from '../components/management/usePriceChecks';
import { useForcePriceChecks } from '../components/management/useForcePriceChecks';

const groups = ['bustelberg', 'toppenberg', 'topselecties', 'imagine_fund', 'test_topselecties'] as const;
const control = 'rounded-lg border border-neutral-700 bg-elevated px-3 py-2 text-xs text-fg-muted transition-colors hover:border-accent-500/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent-500/40 disabled:opacity-50';

export default function AirsCheckPage() {
  const [lang] = useLang();
  const nl = lang === 'nl', text = (en: string, dutch: string) => nl ? dutch : en;
  const management = useMgmtCopy(), copy = usePriceCheckCopy();
  const [rows, setRows] = useState<AirsPortfolioOverview[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [group, setGroup] = useState('all');
  const [query, setQuery] = useState('');
  const [issuesOnly, setIssuesOnly] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [rechecking, setRechecking] = useState<ReadonlySet<string>>(() => new Set());
  const alive = useRef(true), version = useRef(0), fetching = useRef(false);
  const recheckingRef = useRef(new Set<string>());
  const accounts = (rows ?? []).filter(canAnalyseAccount);
  const checks = usePriceChecks(accounts);
  const force = useForcePriceChecks();
  const busy = !!force.progress || rechecking.size > 0;

  const load = useCallback(async () => {
    const request = ++version.current;
    fetching.current = true;
    if (alive.current) setLoading(true);
    try {
      // Read saved daily checks. Opening this page never starts an AIRS or Yahoo refresh.
      const response = await apiFetch(`${API_URL}/api/airs/portfolios/overview`, { noReadCache: true, cache: 'no-store' });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json() as AirsPortfolioOverview[];
      if (!Array.isArray(data)) throw new Error('Invalid account list');
      if (alive.current && request === version.current) { setRows(data); setError(null); }
    } catch (err) {
      if (alive.current && request === version.current) setError(err instanceof Error ? err.message : String(err));
    } finally {
      if (request === version.current) {
        fetching.current = false;
        if (alive.current) setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    alive.current = true;
    void load();
    return () => { alive.current = false; version.current += 1; };
  }, [load]);
  useEffect(() => {
    const refreshSaved = () => {
      if (!busy && !fetching.current && document.visibilityState !== 'hidden') void load();
    };
    const timer = window.setInterval(refreshSaved, 60_000);
    window.addEventListener('focus', refreshSaved);
    return () => { window.clearInterval(timer); window.removeEventListener('focus', refreshSaved); };
  }, [busy, load]);

  const attention = (account: AirsPortfolioOverview) => {
    const check = checks.checks[account.dynamic_portefeuille];
    return !check?.summary || check.error || check.summary.items.some(isExtremeDifference)
      || check.summary.stale > 0 || check.summary.unavailable > 0;
  };
  const filtered = accounts.filter((account) => (group === 'all' || account.management_group === group)
    && account.name.toLocaleLowerCase().includes(query.toLocaleLowerCase()));
  const visible = filtered.filter((account) => !issuesOnly || attention(account))
    .sort((a, b) => a.name.localeCompare(b.name, nl ? 'nl' : 'en'));
  const current = visible.find((account) => account.dynamic_portefeuille === selected);
  const currentCheck = current ? checks.checks[current.dynamic_portefeuille] : undefined;

  const runChecks = async (targets: AirsPortfolioOverview[]) => {
    if (busy || !targets.length) return;
    // An overview read started before this job must not overwrite its completed check.
    version.current += 1; fetching.current = false; setLoading(false);
    setActionError(null);
    const ids = targets.map((account) => account.dynamic_portefeuille);
    checks.beginRefresh(ids);
    const ok = await force.run(targets.map((account) => ({ id: account.dynamic_portefeuille, name: account.name })),
      copy.forceAll, async () => { await checks.refreshMany(ids); if (alive.current) await load(); });
    if (alive.current && ok === false) setActionError(text('Some price checks failed. See the job details.', 'Een of meer koerscontroles zijn mislukt. Bekijk de taakdetails.'));
  };

  const recheckRow = async (account: string, isin: string, name: string) => {
    if (force.progress || recheckingRef.current.has(isin)) return;
    recheckingRef.current.add(isin);
    version.current += 1; fetching.current = false; setLoading(false);
    setRechecking(new Set(recheckingRef.current)); setActionError(null);
    try {
      const { done } = await startJob(`${API_URL}/api/airs/accounts/${encodeURIComponent(account)}/isins/${encodeURIComponent(isin)}/refresh-price/job`,
        `${name} — Yahoo price`, undefined, undefined, { label: name });
      const job = await done;
      await checks.refresh(account);
      if (job.status === 'failed') throw new Error(job.summary ?? 'Price refresh failed');
      if (alive.current) await load();
    } catch (err) {
      if (alive.current) setActionError(err instanceof Error ? err.message : String(err));
    } finally {
      recheckingRef.current.delete(isin);
      if (alive.current) setRechecking(new Set(recheckingRef.current));
    }
  };

  const stamp = (date?: string) => date ? new Date(date).toLocaleString(nl ? 'nl-NL' : 'en-GB') : '—';
  return <main className="mx-auto w-full max-w-[80rem] space-y-5 px-4 py-6 sm:px-8">
    <header className="flex flex-wrap items-start justify-between gap-4">
      <div><h1 className="text-xl font-semibold text-fg-strong">{text('AIRS Check', 'AIRS-controle')}</h1>
        <p className="mt-1 max-w-3xl text-sm text-fg-muted">{text('Compare saved AIRS and Yahoo prices. Daily checks run automatically; refresh only when you need a new comparison.', 'Vergelijk opgeslagen AIRS- en Yahoo-koersen. De dagelijkse controles draaien automatisch; ververs wanneer u een nieuwe vergelijking nodig heeft.')}</p></div>
      <div className="flex flex-wrap gap-2">
        <button type="button" className={control} disabled={loading || busy} onClick={() => void load()}>{text('Reload results', 'Resultaten herladen')}</button>
        <button type="button" className={`${control} border-accent-500/40 text-accent-300`} disabled={!visible.length || busy}
          onClick={() => void runChecks(visible)} title={copy.forceAllHint}>{force.progress ? copy.checking : copy.forceAll}</button>
      </div>
    </header>
    {(error || actionError) && <p role="alert" className="rounded-lg border border-warn-500/30 bg-warn-500/10 p-3 text-xs text-warn-300">{error ?? actionError}</p>}
    <PriceCheckNotice alerts={checks.alerts} names={Object.fromEntries(accounts.map((account) => [account.dynamic_portefeuille, account.name]))} onDismiss={checks.dismissAlerts} />
    <section className="space-y-4 rounded-xl border border-neutral-800/40 bg-card p-4">
      <div className="flex flex-wrap items-center gap-3">
        <label className="flex items-center gap-2 text-xs text-fg-muted">{text('Portfolios', 'Portefeuilles')}
          <select className={control} value={group} disabled={busy} onChange={(event) => setGroup(event.target.value)}>
            <option value="all">{text('All portfolios', 'Alle portefeuilles')}</option>
            {groups.map((key) => <option key={key} value={key}>{management.page.tabs[key].label}</option>)}
          </select></label>
        <input aria-label={text('Search portfolios', 'Portefeuilles zoeken')} className={`${control} min-w-0 flex-1`} value={query}
          disabled={busy} placeholder={text('Search portfolios…', 'Portefeuilles zoeken…')} onChange={(event) => setQuery(event.target.value)} />
        <label className="flex items-center gap-2 text-xs text-fg-muted"><input type="checkbox" checked={issuesOnly} disabled={busy}
          onChange={(event) => setIssuesOnly(event.target.checked)} />{text('Needs attention', 'Aandacht nodig')}</label>
      </div>
      <p className="text-xs text-fg-muted">{visible.length} {text('portfolios', 'portefeuilles')} · {filtered.filter(attention).length} {text('need attention or have no saved check', 'vragen aandacht of hebben nog geen opgeslagen controle')}</p>
      {rows == null ? <p role="status" className="text-sm text-fg-muted">{loading ? copy.checking : text('Could not load portfolios.', 'Portefeuilles laden mislukt.')}</p>
        : <div className="overflow-x-auto"><table className="w-full min-w-[45rem] text-sm">
          <thead><tr className="border-b border-neutral-800/40 text-left text-xs text-fg-muted">
            {[text('Portfolio', 'Portefeuille'), copy.checkedOn, text('Coverage', 'Dekking'), text('Differences ≥5%', 'Verschillen ≥5%'), ''].map((heading, index) => <th key={index} scope="col" className="px-3 py-2 font-medium">{heading}</th>)}
          </tr></thead><tbody className="divide-y divide-neutral-800/30">{visible.map((account) => {
            const check = checks.checks[account.dynamic_portefeuille];
            const count = check?.summary?.items.filter(isExtremeDifference).length;
            return <tr key={account.dynamic_portefeuille} className={selected === account.dynamic_portefeuille ? 'bg-accent-500/5' : 'hover:bg-overlay/[0.03]'}>
              <td className="px-3 py-3 font-medium text-fg-strong">{account.name}</td>
              <td className="px-3 py-3 text-xs text-fg-muted">{stamp(check?.checkedAt)}</td>
              <td className="px-3 py-3 font-mono text-xs tabular-nums">{check?.summary ? `${check.summary.checked}/${check.summary.total}` : '—'}
                {check?.error && <span className="ml-2 text-warn-300">{copy.failed}</span>}
                {!check && <span className="ml-2 text-fg-muted">{text('Not checked', 'Niet gecontroleerd')}</span>}</td>
              <td className={`px-3 py-3 font-mono tabular-nums ${count ? 'text-warn-300' : 'text-fg-muted'}`}>{count ?? '—'}
                {!!check?.summary?.stale && <span className="ml-2 text-xs">{copy.stale(check.summary.stale)}</span>}</td>
              <td className="px-3 py-3 text-right"><button type="button" aria-expanded={selected === account.dynamic_portefeuille}
                className={control} aria-label={`${copy.viewPrices} · ${account.name}`} onClick={() => setSelected(selected === account.dynamic_portefeuille ? null : account.dynamic_portefeuille)}>{copy.viewPrices}</button></td>
            </tr>;
          })}</tbody></table>
          {!visible.length && <p className="py-5 text-sm text-fg-muted">{text('No portfolios match these filters.', 'Geen portefeuilles voor deze filters.')}</p>}
        </div>}
    </section>
    {current && <section aria-label={current.name} className="space-y-3">
      <div className="flex items-center justify-between gap-3"><h2 className="text-base font-semibold text-fg-strong">{current.name}</h2>
        <button type="button" className={control} onClick={() => setSelected(null)}>{copy.close}</button></div>
      {currentCheck ? <PriceCheckDetails state={currentCheck} showAll refreshing={busy} recheckingIsins={rechecking}
        onRetry={() => void runChecks([current])}
        onRecheckRow={force.progress ? undefined : (isin, name) => void recheckRow(current.dynamic_portefeuille, isin, name)} />
        : <div className="space-y-3 rounded-lg border border-neutral-800/40 bg-card p-4"><p className="text-sm text-fg-muted">{text('No saved price check is available for this portfolio yet.', 'Voor deze portefeuille is nog geen opgeslagen koerscontrole beschikbaar.')}</p>
          <button type="button" className={control} disabled={busy} onClick={() => void runChecks([current])}>{busy ? copy.checking : copy.retry}</button></div>}
    </section>}
  </main>;
}
