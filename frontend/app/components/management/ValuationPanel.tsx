'use client';

import { useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { useLang } from '../../../lib/i18n';
import type { components } from '../../../lib/api-types';
import WorkbookValuationTable from './WorkbookValuationTable';
import { COLUMN_STYLE } from './valuationWorkbookColumns';
import type { Workbook } from './valuationWorkbook';
import { ExpectedJobFailure, startLocalJob } from '../../../lib/stores/jobs';
import { runSSE } from '../../../lib/stream';

type Portfolio = components['schemas']['ValuationPortfolio'];
type Bundle = components['schemas']['ValuationBundle'];
const selectionKey = (portfolio: Portfolio) => portfolio.account ?? String(portfolio.id);

export default function ValuationPanel() {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [selected, setSelected] = useState('');
  const [bundle, setBundle] = useState<{ id: string; data: Bundle } | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [refreshingIsin, setRefreshingIsin] = useState<string | null>(null);
  const activeSelection = useRef('');
  useEffect(() => () => { activeSelection.current = ''; }, []);
  const selectedId = portfolios.find(portfolio => selectionKey(portfolio) === selected)?.id;
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
    if (!selected || selectedId == null) return;
    let active = true;
    const controller = new AbortController();
    const timeout = setTimeout(() => { controller.abort(); if (active) { setError(nl ? 'Portefeuille laden duurt te lang. Probeer opnieuw.' : 'Portfolio data took too long to load. Select it again to retry.'); setLoading(false); } }, 45000);
    void apiFetch(`${API_URL}/api/airs/valuation/portfolios/${selectedId}/dashboard`, { cache: 'no-store', noReadCache: true, signal: controller.signal })
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
  }, [selected, selectedId, nl]);
  const data = bundle?.id === selected ? bundle.data : null;
  const workbook = data?.workbook as Workbook | null | undefined;
  const rows = [...(data?.rows ?? [])].sort((a, b) => a.name.localeCompare(b.name));
  const refreshPortfolio = () => {
    if (loading) return;
    if (selectedId == null) {
      setLoading(true); setError('');
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
            }
            if (event.type === 'error') failure = event.message ?? 'Portfolio preparation failed.';
          }, signal);
          if (signal.aborted) return;
          if (failure || !complete) throw new ExpectedJobFailure(failure || 'Portfolio preparation ended before completion.',
            nl ? 'Niet alle portefeuilles konden worden voorbereid.' : 'Some portfolios could not be prepared.');
          return nl ? 'Portefeuilles voorbereid.' : 'Portfolios prepared.';
        } catch (cause) {
          if (!signal.aborted) setError(cause instanceof Error ? cause.message : String(cause));
          throw cause;
        } finally { setLoading(false); }
      });
      return;
    }
    const id = selectedId, selection = selected;
    setLoading(true); setError('');
    startLocalJob(nl ? 'Waardering vernieuwen' : 'Refresh valuation', 'valuation.portfolio', async (signal, report) => {
      let complete = false;
      let failure = '';
      let progress = { done: 0, total: 0 };
      const current = () => activeSelection.current === selection;
      try {
        await runSSE(`${API_URL}/api/airs/valuation/portfolios/${id}/dashboard/refresh`, { method: 'POST' }, raw => {
          const event = raw as { type: string; done?: number; total?: number; message?: string; isin?: string;
            row?: Bundle['rows'][number]; rows?: Bundle['rows']; data?: Bundle; failures?: string[] };
          if (event.type === 'progress') {
            progress = { done: event.done ?? 0, total: event.total ?? 0 };
            report({ ...progress, message: event.message });
            if (current()) setRefreshingIsin(event.isin ?? null);
          } else if (event.type === 'holdings' && event.rows && current()) {
            setBundle(existing => existing?.id === selection ? { ...existing, data: { ...existing.data, rows: event.rows! } } : existing);
          } else if (event.type === 'row' && event.row && current()) {
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
          } else if (event.type === 'error') failure = event.message ?? 'Portfolio refresh failed.';
        }, signal);
        if (signal.aborted) return;
        if (failure || !complete) throw new ExpectedJobFailure(failure || 'Portfolio refresh ended before completion.',
          nl ? 'Niet alle brongegevens konden worden vernieuwd.' : 'Some source data could not be refreshed.');
        return nl ? 'Portefeuille bijgewerkt.' : 'Portfolio updated.';
      } catch (cause) {
        if (current() && !signal.aborted) setError(cause instanceof Error ? cause.message : String(cause));
        throw cause;
      } finally { if (current()) { setRefreshingIsin(null); setLoading(false); } }
    });
  };
  return <section className="space-y-4" aria-label={nl ? 'Waardering' : 'Valuation'}>
    <div className="flex flex-wrap items-end gap-3">
      <label className="space-y-1 text-sm"><span className="block">{nl ? 'Portefeuille' : 'Portfolio'}</span>
        <select disabled={loading} value={selected} onChange={event => { activeSelection.current = event.target.value; setSelected(event.target.value); setLoading(!!event.target.value); setError(''); }} className="rounded-lg border border-neutral-800/40 bg-card px-3 py-2">
          <option value="">{nl ? 'Selecteer een portefeuille…' : 'Select a portfolio…'}</option>
          {portfolios.map(portfolio => <option key={selectionKey(portfolio)} value={selectionKey(portfolio)}
            disabled={portfolio.id == null} title={portfolio.unavailable_reason ?? undefined}>
            {portfolio.name}{portfolio.id == null ? (nl ? ' — geen gekoppeld AIRS-model' : ' — no linked AIRS model') : ''}
          </option>)}
        </select>
      </label>
      <div className="flex flex-wrap items-center gap-3">
        <button type="button" onClick={refreshPortfolio} disabled={loading} className="rounded-lg border border-neutral-800/40 px-3 py-2 text-sm disabled:opacity-50">{loading ? (nl ? 'Berekenen…' : 'Calculating…') : (nl ? 'Vernieuwen' : 'Refresh')}</button>
        <div className="flex flex-wrap items-center gap-3 text-xs" aria-label={nl ? 'Kolomtypen' : 'Column types'}>
          {(['data', 'assumption', 'calculation'] as const).map(kind => <span key={kind} className={`rounded border-t-2 px-3 py-2 ${COLUMN_STYLE[kind]}`}>
            {kind === 'data' ? (nl ? 'Blauw · directe gegevens' : 'Blue · direct data') : kind === 'assumption'
              ? (nl ? 'Groen · aannames' : 'Green · assumptions') : (nl ? 'Geel · berekening' : 'Yellow · calculation')}
          </span>)}
        </div>
      </div>
    </div>
    {(selected || error) && <>
      {error && <p role="alert" className="text-sm text-neg-300">{error}</p>}
      {data && !workbook && <p role="alert" className="text-sm text-neg-300">{nl ? 'De portefeuillebundel bevat geen werkmapgegevens. Vernieuw de portefeuille.' : 'The portfolio bundle is missing workbook data. Refresh the portfolio.'}</p>}
      {workbook && selectedId != null && <WorkbookValuationTable key={selected} workbookData={workbook} refreshingIsin={refreshingIsin}
        portfolioId={selectedId} rows={rows} nl={nl} portfolioName={portfolios.find(row => selectionKey(row) === selected)?.name ?? ''} />}
    </>}
  </section>;
}
