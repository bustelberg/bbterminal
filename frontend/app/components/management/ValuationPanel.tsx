'use client';

import { useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { useLang } from '../../../lib/i18n';
import type { components } from '../../../lib/api-types';
import WorkbookValuationTable from './WorkbookValuationTable';
import type { Workbook } from './valuationWorkbook';
import { ExpectedJobFailure, jobsStore, startLocalJob } from '../../../lib/stores/jobs';
import { runSSE } from '../../../lib/stream';
import { valuationControl, valuationField, valuationToolbar } from './valuationControlStyles';

type Portfolio = components['schemas']['ValuationPortfolio'];
type Bundle = components['schemas']['ValuationBundle'];
const selectionKey = (portfolio: Portfolio) => portfolio.account ?? String(portfolio.id);
const ALL_PORTFOLIOS = 'all';

export default function ValuationPanel() {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [selected, setSelected] = useState('');
  const [bundle, setBundle] = useState<{ id: string; data: Bundle } | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [allRevision, setAllRevision] = useState(0);
  const refreshing = jobsStore.use(state => state.jobs.some(job =>
    (job.kind === 'valuation.portfolio' || job.kind === 'valuation.prepare') && job.status === 'running'));
  const [refreshingIsin, setRefreshingIsin] = useState<string | null>(null);
  const [workbookUpdate, setWorkbookUpdate] = useState<Workbook | null>(null);
  const activeSelection = useRef('');
  useEffect(() => {
    activeSelection.current = selected;
    return () => { activeSelection.current = ''; };
  }, [selected]);
  const selectedId = portfolios.find(portfolio => selectionKey(portfolio) === selected)?.id;
  const showAll = selected === ALL_PORTFOLIOS;
  const visiblePortfolios = portfolios.filter(portfolio => !/\b(?:defensief|neutraal|beperkt\s+offensief)$/i.test(portfolio.name.trim()));
  useEffect(() => {
    let active = true;
    void apiFetch(`${API_URL}/api/airs/valuation/portfolios`, { cache: 'no-store', noReadCache: true })
      .then(async response => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return await response.json() as Portfolio[];
      }).then(rows => { if (active) setPortfolios(rows); })
      .catch(cause => { if (active) setError(String(cause)); });
    return () => { active = false; };
  }, []);
  useEffect(() => {
    if (!selected || (!showAll && selectedId == null)) return;
    let active = true;
    const controller = new AbortController();
    const timeout = setTimeout(() => { controller.abort(); if (active) { setError(nl ? 'Portefeuille laden duurt te lang. Probeer opnieuw.' : 'Portfolio data took too long to load. Select it again to retry.'); setLoading(false); } }, 45000);
    void apiFetch(`${API_URL}/api/airs/valuation/portfolios/${showAll ? ALL_PORTFOLIOS : selectedId}/dashboard`, { cache: 'no-store', noReadCache: true, signal: controller.signal })
      .then(async response => {
        const body = await response.json();
        if (!response.ok) throw new Error(typeof body.detail === 'string' ? body.detail : `HTTP ${response.status}`);
        return body as Bundle;
      }).then(async data => {
        if (!active || controller.signal.aborted) return;
        setBundle({ id: selected, data });

      })
      .catch(cause => { if (active && !controller.signal.aborted) setError(cause instanceof Error ? cause.message : String(cause)); })
      .finally(() => { clearTimeout(timeout); if (active) setLoading(false); });
    return () => { active = false; clearTimeout(timeout); controller.abort(); };
  }, [selected, selectedId, showAll, nl, allRevision]);
  const data = bundle?.id === selected ? bundle.data : null;
  const workbook = data?.workbook as Workbook | null | undefined;
  const rows = [...(data?.rows ?? [])].sort((a, b) => a.name.localeCompare(b.name));
  const refreshPortfolio = () => {
    if (loading || refreshing) return;
    if (selectedId == null) {
      setError('');
      startLocalJob(nl ? 'Waardering voorbereiden' : 'Prepare valuations', 'valuation.prepare', async (signal, report) => {
        let complete = false;
        let failure = '';
        try {
          await runSSE(`${API_URL}/api/airs/valuation/prepare/refresh`, { method: 'POST' }, raw => {
            const event = raw as { type: string; done?: number; total?: number; message?: string; portfolios?: Portfolio[]; failures?: string[] };
            if (event.type === 'progress') report({ done: event.done ?? 0, total: event.total ?? 0, message: event.message });
            if (event.type === 'complete') {
              complete = true;
              setPortfolios(event.portfolios ?? []);
              failure = event.failures?.join('\n') ?? '';
              if (activeSelection.current === ALL_PORTFOLIOS) setAllRevision(current => current + 1);
            }
            if (event.type === 'error') failure = event.message ?? 'Portfolio preparation failed.';
          }, signal);
          if (signal.aborted) return;
          if (failure || !complete) throw new ExpectedJobFailure(failure || 'Portfolio preparation ended before completion.',
            nl ? 'Niet alle portefeuilles konden worden voorbereid.' : 'Some portfolios could not be prepared.');
          return nl ? 'Portefeuilles voorbereid.' : 'Portfolios prepared.';
        } finally { setLoading(false); }
      });
      return;
    }
    const id = selectedId, selection = selected;
    setError(''); setWorkbookUpdate(null);
    startLocalJob(nl ? 'Waardering vernieuwen' : 'Refresh valuation', 'valuation.portfolio', async (signal, report) => {
      let complete = false;
      let failure = '';
      let outcome = '';
      let progress = { done: 0, total: 0 };
      const current = () => activeSelection.current === selection;
      try {
        await runSSE(`${API_URL}/api/airs/valuation/portfolios/${id}/dashboard/refresh`, { method: 'POST' }, raw => {
          const event = raw as { type: string; done?: number; total?: number; message?: string; isin?: string;
            row?: Bundle['rows'][number]; rows?: Bundle['rows']; data?: Bundle; failures?: string[]; workbook?: Workbook;
            counts?: { succeeded: number; failed: number; total: number } };
          if (event.type === 'progress') {
            progress = { done: event.done ?? 0, total: event.total ?? 0 };
            report({ ...progress, message: event.message });
            if (current()) setRefreshingIsin(event.isin ?? null);
          } else if (event.type === 'holdings' && event.rows && current()) {
            setBundle(existing => existing?.id === selection ? { ...existing, data: { ...existing.data, rows: event.rows! } } : existing);
          } else if (event.type === 'row' && event.row && current()) {
            if (event.workbook) {
              const update = event.workbook;
              setWorkbookUpdate(previous => {
                if (!previous || previous.portfolio_id !== update.portfolio_id) return update;
                const rows = new Map(previous.rows.map(row => [row.isin, row]));
                for (const row of update.rows) rows.set(row.isin, row);
                return { ...update, rows: [...rows.values()] };
              });
            }
            const row = event.row;
            setBundle(existing => existing?.id === selection ? { ...existing, data: { ...existing.data,
              rows: existing.data.rows.some(item => item.isin === row.isin)
                ? existing.data.rows.map(item => item.isin === row.isin ? row : item) : [...existing.data.rows, row] } } : existing);
          } else if (event.type === 'row_error') {
            report({ ...progress, message: event.message });
          } else if (event.type === 'complete' && event.data) {
            complete = true;
            if (current()) setBundle({ id: selection, data: event.data });
            failure = event.failures?.join('\n') ?? '';
            const total = event.counts?.total ?? event.data.rows.length;
            const failed = event.counts?.failed ?? (event.failures?.length ?? 0);
            const succeeded = event.counts?.succeeded ?? Math.max(0, total - failed);
            outcome = nl ? `${succeeded}/${total} bedrijven vernieuwd; ${failed} mislukt. Succesvolle updates zijn opgeslagen.`
              : `${succeeded}/${total} companies refreshed; ${failed} failed. Successful updates saved.`;
            const otherFailures = Math.max(0, (event.failures?.length ?? 0) - failed);
            if (otherFailures) outcome += nl ? ` ${otherFailures} aanvullende controles mislukt.` : ` ${otherFailures} additional checks failed.`;
            for (const detail of event.failures ?? []) report({ done: total, total, message: detail });
          } else if (event.type === 'error') failure = event.message ?? 'Portfolio refresh failed.';
        }, signal);
        if (signal.aborted) return;
        if (failure || !complete) throw new ExpectedJobFailure(outcome || failure || 'Portfolio refresh ended before completion.',
          nl ? 'Bekijk de details voor de fouten.' : 'See details for the errors.');
        return outcome;
      } finally { if (current()) setRefreshingIsin(null); }
    });
  };
  return <section className="space-y-2" aria-label={nl ? 'Waardering' : 'Valuation'}>
    <div className={valuationToolbar}>
      <label className={valuationField}><span>{nl ? 'Portefeuille' : 'Portfolio'}</span>
        <select disabled={loading || refreshing} value={selected} onChange={event => { activeSelection.current = event.target.value; setSelected(event.target.value); setLoading(!!event.target.value); setError(''); }} className={`${valuationControl} min-w-0`}>
          <option value="">{nl ? 'Selecteer een portefeuille…' : 'Select a portfolio…'}</option>
          <option value={ALL_PORTFOLIOS}>{nl ? 'Alle' : 'All'}</option>
          {visiblePortfolios.map(portfolio => <option key={selectionKey(portfolio)} value={selectionKey(portfolio)}
            disabled={portfolio.id == null} title={portfolio.unavailable_reason ?? undefined}>
            {portfolio.name}{portfolio.id == null ? (nl ? ' — geen gekoppeld AIRS-model' : ' — no linked AIRS model') : ''}
          </option>)}
        </select>
      </label>
      <button type="button" onClick={refreshPortfolio} disabled={loading || refreshing} className={`${valuationControl} enabled:cursor-pointer enabled:hover:border-accent-500/50`}>{refreshing ? (nl ? 'Vernieuwen…' : 'Refreshing…') : loading ? (nl ? 'Laden…' : 'Loading…') : (nl ? 'Vernieuwen' : 'Refresh')}</button>
    </div>
    {(selected || error) && <>
      {error && <p role="alert" className="text-sm text-neg-300">{error}</p>}
      {data && !workbook && <p role="alert" className="text-sm text-neg-300">{nl ? 'De portefeuillebundel bevat geen werkmapgegevens. Vernieuw de portefeuille.' : 'The portfolio bundle is missing workbook data. Refresh the portfolio.'}</p>}
      {showAll && data && <p className="text-xs text-fg-muted">{nl
        ? 'Alle bedrijven in één overzicht met standaardwaarderingsaannames. Wijzig aannames in de afzonderlijke portefeuille.'
        : 'All companies in one view using default valuation assumptions. Edit assumptions in the individual portfolio.'}</p>}
      {showAll && data && Array.isArray(data.evidence?.unavailable_portfolios) && data.evidence.unavailable_portfolios.length > 0 &&
        <p className="text-xs text-fg-muted">{nl ? 'Geen volledige posities voor' : 'Incomplete holdings for'} {data.evidence.unavailable_portfolios.join(', ')}.</p>}
      {workbook && (showAll || selectedId != null) && <WorkbookValuationTable key={selected} workbookData={workbook} workbookUpdate={workbookUpdate} refreshingIsin={refreshing ? refreshingIsin : null}
        portfolioId={showAll ? workbook.portfolio_id : selectedId!} rows={rows} nl={nl} readOnly={showAll} />}
    </>}
  </section>;
}
