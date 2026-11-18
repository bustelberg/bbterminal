'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { workbookValuation, type CalculationKey, type Workbook, type WorkbookRow, type CompanyAssumption } from './valuationWorkbook';
import { COLUMN_STYLE, WORKBOOK_PANELS, columnsForPanel, panelForInput, type WorkbookColumn, type WorkbookPanelKey } from './valuationWorkbookColumns';
import ValuationFormulaExplanation from './ValuationFormulaExplanation';
import { selectingTabText } from './valuationTabSelection';
import { valuationControl, valuationField, valuationToolbar } from './valuationControlStyles';

type Quote = { name: string; isin?: string | null; price?: number | null; price_date?: string | null; currency?: string | null };
const control = 'rounded-lg border border-neutral-700 bg-elevated px-2 py-1.5 text-xs';
const SIGNAL_STYLE: Record<string, string> = {
  BUY: 'bg-pos-500/15 text-pos-400',
  CONSIDER: 'bg-warn-500/15 text-warn-400',
  'DO NOT BUY': 'bg-neg-500/15 text-neg-400',
  REVIEW: 'bg-overlay/10 text-fg-muted',
};
const sourceFields = (row?: WorkbookRow) => row ? {
  ticker: row.ticker, quote: row.quote, forecast: row.forecast, fx_rates: row.fx_rates,
  assumptions: row.assumptions, seed_source: row.seed_source,
} : {};
const number = (v: number | null | undefined, suffix = '') => v == null || !Number.isFinite(v) ? '—' : `${v.toFixed(2)}${suffix}`;

export default function WorkbookValuationTable({ portfolioId, rows, nl, refreshingIsin = null, workbookUpdate = null, workbookData, readOnly = false }: {
  portfolioId: number; rows: Quote[]; nl: boolean; refreshingIsin?: string | null; workbookUpdate?: Workbook | null; workbookData?: Workbook | null; readOnly?: boolean;
}) {
  const [workbook, setWorkbook] = useState<Workbook | null>(workbookData ?? null);
  const hasWorkbook = workbook !== null;
  const [error, setError] = useState('');
  const [loadAttempt, setLoadAttempt] = useState(0);
  const refreshedRows = useRef(new Map<string, WorkbookRow>());
  const [busy, setBusy] = useState(false);
  const [panel, setPanel] = useState<WorkbookPanelKey>('company');
  const [search, setSearch] = useState('');
  const [sort, setSort] = useState('name');
  const [columnSort, setColumnSort] = useState<{ key: string; descending: boolean } | null>(null);
  const [formulaIsin, setFormulaIsin] = useState('');
  const [formula, setFormula] = useState<WorkbookColumn | null>(null);
  const formulaAnchor = useRef<HTMLElement | null>(null);
  const [formulaPosition, setFormulaPosition] = useState({ left: 12, top: 12, width: 1120, maxHeight: 800 });
  const [focusedInput, setFocusedInput] = useState<string | null>(null);
  const table = useRef<HTMLDivElement>(null);
  const viewport = useRef<HTMLDivElement>(null);
  const root = useRef<HTMLDivElement>(null);
  const parametersForm = useRef<HTMLDetailsElement>(null);
  const alive = useRef(true);
  const t = useCallback((en: string, dutch: string) => nl ? dutch : en, [nl]);
  const openFormula = (column: WorkbookColumn, isin: string, anchor: HTMLElement) => {
    if (formula && formulaAnchor.current === anchor) {
      setFormula(null);
      return;
    }
    const container = viewport.current;
    if (!container) return;
    const bounds = container.getBoundingClientRect();
    const clicked = anchor.getBoundingClientRect();
    const visibleWidth = container.clientWidth || bounds.width || window.innerWidth;
    const visibleHeight = container.clientHeight || bounds.height || 520;
    const width = Math.min(1120, Math.max(0, visibleWidth - 24));
    const left = Math.max(12, Math.min(clicked.left - bounds.left, visibleWidth - width - 12));
    const below = clicked.bottom - bounds.top + 8;
    const maxHeight = Math.max(0, Math.min(800, visibleHeight - 24));
    const top = Math.max(12, Math.min(below, visibleHeight - maxHeight - 12));
    setFormulaPosition({ left: container.scrollLeft + left, top: container.scrollTop + top,
      width, maxHeight });
    formulaAnchor.current = anchor;
    setFormulaIsin(isin);
    setFormula(column);
  };
  const url = `${API_URL}/api/airs/valuation/portfolios/${portfolioId}/workbook`;
  useEffect(() => {
    alive.current = true;
    if (workbookData !== undefined) {
      setWorkbook(workbookData);
      return () => { alive.current = false; };
    }
    let active = true;
    const controller = new AbortController();
    const timeout = setTimeout(() => { controller.abort(); if (active) setError(t('Valuation inputs took too long to load. Retry or refresh the source data.', 'Waarderingsinvoer laden duurt te lang. Probeer opnieuw of vernieuw de brongegevens.')); }, 30000);
    void apiFetch(url, { cache: 'no-store', noReadCache: true, signal: controller.signal }).then(async response => {
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(typeof body.detail === 'string' ? body.detail : t('Valuation inputs could not be loaded.', 'Waarderingsinvoer kon niet worden geladen.'));
      }
      return await response.json() as Workbook;
    }).then(data => { if (active && !controller.signal.aborted) { setError(''); setWorkbook({ ...data, rows: data.rows.map(row => ({ ...row, ...sourceFields(refreshedRows.current.get(row.isin ?? '')) })) }); } })
      .catch(cause => { if (active && !controller.signal.aborted) setError(String(cause.message ?? cause)); })
      .finally(() => clearTimeout(timeout));
    return () => { active = false; alive.current = false; clearTimeout(timeout); controller.abort(); };
  }, [url, t, loadAttempt, workbookData]);
  useEffect(() => {
    if (workbookData !== undefined || !hasWorkbook) return;
    const controller = new AbortController();
    void apiFetch(`${url}/memberships`, { noReadCache: true, signal: controller.signal }).then(async response => {
      if (!response.ok) return;
      const data = await response.json();
      if (!data.memberships || !data.legend || controller.signal.aborted) return;
      setWorkbook(current => current && ({ ...current, membership_legend: data.legend,
        rows: current.rows.map(row => {
          const confirmed = data.memberships[row.isin ?? ''] ?? {};
          const unknown = data.legend.filter((item: { code: string; status: string }) => item.status !== 'complete' && !confirmed[item.code]).map((item: { code: string }) => item.code);
          return { ...row, selections: Object.keys(confirmed), membership_profiles: confirmed,
            membership_unknown_codes: unknown, membership_complete: unknown.length === 0 };
        }) }));
    }).catch(() => { /* Missing membership sources remain explicitly unconfirmed. */ });
    return () => controller.abort();
  }, [url, loadAttempt, hasWorkbook, workbookData]);
  useEffect(() => {
    if (!workbookUpdate || workbookUpdate.portfolio_id !== portfolioId) return;
    for (const row of workbookUpdate.rows) {
      if (row.isin) refreshedRows.current.set(row.isin, row);
    }
    setWorkbook(current => {
      if (!current) return workbookUpdate;
      const updates = new Map(workbookUpdate.rows.map(row => [row.isin, row]));
      const rows = current.rows.map(row => {
        const update = updates.get(row.isin);
        updates.delete(row.isin);
        return update ? { ...row, ...sourceFields(update) } : row;
      });
      return { ...current, as_of: workbookUpdate.as_of, rows: [...rows, ...updates.values()] };
    });
  }, [workbookUpdate, portfolioId]);
  useEffect(() => {
    if (viewport.current) { viewport.current.scrollTop = 0; viewport.current.scrollLeft = 0; }
  }, [panel]);
  useEffect(() => {
    if (!focusedInput) return;
    const parameter = parametersForm.current?.querySelector<HTMLInputElement>(`input[name="${focusedInput}"]`);
    if (parameter && parametersForm.current) parametersForm.current.open = true;
    const target = parameter ?? table.current?.querySelector<HTMLElement>(`th[data-column="${focusedInput}"]`)
      ?? root.current?.querySelector<HTMLElement>(`[data-column="${focusedInput}"]`);
    target?.scrollIntoView?.({ block: 'nearest', inline: 'center' });
    parameter?.focus();
  }, [panel, focusedInput]);
  const columns = columnsForPanel(panel);
  const membershipGaps = (workbook?.membership_legend ?? []).filter(item => item.status !== 'complete');
  const coverageNote = t('Displayed codes are confirmed memberships. Other memberships cannot be fully checked for: ',
    'Getoonde codes zijn bevestigde lidmaatschappen. Overige lidmaatschappen kunnen niet volledig worden gecontroleerd voor: ')
    + membershipGaps.map(item => `${item.code} (${item.name})`).join(', ');
  const scenarioYear = Number(workbook?.as_of?.slice(0, 4) ?? new Date().getFullYear()) + (workbook?.parameters.years ?? 3);
  const entries = rows.map(quote => {
    const row = workbook?.rows.find(item => item.isin != null && item.isin === quote.isin);
    const result = row && workbook ? workbookValuation(row, row.quote ?? {}, workbook.parameters, workbook.as_of) : null;
    const warning = row?.assumptions.review_required ? t('Analyst review required', 'Analistencontrole vereist')
      : result?.reason ?? (result?.signal === '?' ? t('Complete valuation assumptions', 'Vul de waarderingsaannames aan')
        : !row ? t('Valuation inputs unavailable', 'Waarderingsinvoer ontbreekt')
          : result?.cagr == null ? t('Scenario assumptions incomplete', 'Scenarioaannames onvolledig') : null);
    const source = row?.quote ?? {};
    const a = row?.assumptions, f = row?.forecast;
    const signals = nl ? { '?': '?', REVIEW: 'NADER TOETSEN', BUY: 'KOOP', CONSIDER: 'TWIJFEL', 'DO NOT BUY': 'NIET KOPEN' }
      : { '?': '?', REVIEW: 'REVIEW', BUY: 'BUY', CONSIDER: 'CONSIDER', 'DO NOT BUY': 'DO NOT BUY' };
    const cells: Record<string, string | number | null | undefined> = {
      company: quote.name, ticker: row?.ticker, selections: row?.selections.join(' | '),
      raw_price: source.price, quote_currency: source.currency, price_date: source.price_date, price_source: source.source,
      fy1: f?.fy1, fy1_date: f?.fy1_date, fy2: f?.fy2, fy2_date: f?.fy2_date,
      forecast_currency: f?.currency, forecast_basis: f?.basis_label, forecast_source: f?.source,
      manual_basis: a?.manual_basis, manual_currency: a?.basis_currency, manual_label: a?.basis_label,
      manual_date: a?.basis_date, manual_source: a?.basis_source,
      basis_kind: a?.basis_kind === 'manual' ? t('Manual source basis', 'Handmatige bronbasis') : 'GuruFocus NTM EPS',
      months_fy1: a?.months_fy1, basis_fx: a?.basis_fx, growth: a?.growth, fair_multiple: a?.fair_multiple,
      exit_multiple: a?.exit_multiple, review_required: a?.review_required == null ? '—' : a.review_required ? t('YES', 'JA') : t('NO', 'NEE'),
      risk_note: a?.risk_note, years: workbook?.parameters.years, safety_margin: workbook?.parameters.safety_margin,
      ...result, signal: signals[result?.signal as keyof typeof signals ?? '?'],
    };
    return { quote, row, result, warning, cells };
  }).filter(({ quote, row }) => `${quote.name} ${row?.ticker ?? ''}`.toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => {
      const { key, descending } = columnSort ?? { key: sort === 'name' ? 'company' : sort, descending: sort !== 'name' };
      const left = a.cells[key], right = b.cells[key];
      const missing = (value: typeof left) => value == null || value === '' || value === '—' || value === '?';
      if (missing(left) !== missing(right)) return missing(left) ? 1 : -1;
      const order = missing(left) ? 0 : typeof left === 'number' && typeof right === 'number'
        ? left - right : String(left).localeCompare(String(right), nl ? 'nl' : 'en', { numeric: true });
      return (descending ? -order : order) || a.quote.name.localeCompare(b.quote.name);
    });

  const save = async (parameters: Workbook['parameters'], changed?: WorkbookRow) => {
    if (!workbook || busy || readOnly) return;
    setBusy(true); setError('');
    try {
      const companies = Object.fromEntries(workbook.rows.filter(row => row.isin).map(row => [row.isin!,
        changed?.isin === row.isin ? changed.assumptions : row.assumptions]));
      const response = await apiFetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ parameters, companies }) });
      if (!response.ok) throw new Error(t('Assumptions could not be saved. Your changes remain open.', 'Aannames konden niet worden opgeslagen. Uw wijzigingen blijven open.'));
      const saved = await response.json() as { parameters: Workbook['parameters'];
        companies: Record<string, CompanyAssumption>; updated_at: string };
      if (alive.current) {
        setWorkbook(current => current && ({ ...current, parameters: saved.parameters, updated_at: saved.updated_at,
          rows: current.rows.map(row => ({ ...row,
            assumptions: row.isin && saved.companies[row.isin] ? saved.companies[row.isin] : row.assumptions })) }));
      }
    } catch (cause) { if (alive.current) setError(String(cause instanceof Error ? cause.message : cause)); }
    finally { if (alive.current) setBusy(false); }
  };
  const numeric = (form: FormData, key: string, percent = false) => {
    const raw = String(form.get(key) ?? '').trim();
    return raw === '' ? null : Number(raw) / (percent ? 100 : 1);
  };
  const assumptionCell = (column: WorkbookColumn, row: WorkbookRow, company: string) => {
    if (readOnly) return null;
    if (panel === 'scenario' && column.key === 'growth') return null;
    const manualFields: Record<string, keyof CompanyAssumption> = {
      manual_basis: 'manual_basis', manual_currency: 'basis_currency', manual_label: 'basis_label',
      manual_date: 'basis_date', manual_source: 'basis_source',
    };
    const field = column.kind === 'assumption' ? column.key as keyof CompanyAssumption : manualFields[column.key];
    if (!field) return null;
    const original = row.assumptions[field];
    const label = `${nl ? column.nl : column.en} · ${company}`;
    const disabled = busy || !row.isin || refreshingIsin === row.isin;
    const commit = (value: CompanyAssumption[keyof CompanyAssumption]) => {
      const previous = original ?? (field === 'review_required' ? false : field === 'basis_kind' ? 'forward_eps'
        : field === 'risk_note' || field === 'basis_label' ? '' : null);
      if (!workbook || value === previous) return;
      void save(workbook.parameters, { ...row, assumptions: { ...row.assumptions, [field]: value } });
    };
    if (field === 'review_required') return <input type="checkbox" aria-label={label} disabled={disabled}
      checked={original === true} onChange={event => commit(event.target.checked)} />;
    if (field === 'basis_kind') return <select aria-label={label} disabled={disabled} className={control}
      value={original === 'manual' ? 'manual' : 'forward_eps'} onChange={event => commit(event.target.value as CompanyAssumption['basis_kind'])}>
      <option value="forward_eps">GuruFocus NTM EPS</option><option value="manual">{t('Manual source basis', 'Handmatige bronbasis')}</option>
    </select>;
    const numeric = ['growth', 'fair_multiple', 'exit_multiple', 'basis_fx', 'months_fy1', 'manual_basis'].includes(field);
    const value = original == null ? '' : numeric ? (Number(original) * (field === 'growth' ? 100 : 1)).toFixed(1) : String(original);
    const min = field === 'growth' ? -99.999 : field === 'months_fy1' ? 0
      : ['fair_multiple', 'exit_multiple', 'basis_fx'].includes(field) ? .000001 : undefined;
    const max = field === 'growth' ? 1000 : field === 'months_fy1' ? 12 : undefined;
    return <span className="inline-flex items-center gap-1">
      <input key={`${field}:${value}`} aria-label={label} disabled={disabled} type={numeric ? 'number' : 'text'}
        className={`${control} ${field === 'risk_note' ? 'w-64' : numeric ? 'w-20' : 'w-40'} disabled:opacity-50`}
        defaultValue={value} placeholder="—" step="any" min={min} max={max}
        maxLength={field === 'risk_note' ? 2000 : field === 'basis_source' ? 1000 : field === 'basis_currency' ? 10 : field === 'basis_date' ? 100 : 200}
        onBlur={event => {
          if (!event.currentTarget.reportValidity()) return;
          const raw = event.currentTarget.value.trim();
          if (raw === value) return;
          commit(numeric ? (raw === '' ? null : Number(raw) / (field === 'growth' ? 100 : 1))
            : raw || (field === 'risk_note' || field === 'basis_label' ? '' : null));
        }}
        onKeyDown={event => {
          if (event.key === 'Enter') { event.preventDefault(); event.currentTarget.blur(); }
          if (event.key === 'Escape') { event.currentTarget.value = String(value); event.currentTarget.blur(); }
        }} />
      {field === 'growth' && <span>%</span>}
    </span>;
  };
  const tabs = <nav className={valuationToolbar} aria-label={t('Valuation panels', 'Waarderingspanelen')}>
      {WORKBOOK_PANELS.map(item => <button key={item.key} type="button" aria-pressed={panel === item.key}
        className={`${valuationControl} cursor-pointer select-text hover:border-accent-500/50 ${item.key === 'parameters' ? 'ml-auto' : ''} aria-pressed:border-accent-500/50 aria-pressed:text-accent-300`}
        onClick={event => { if (selectingTabText(event)) return; setPanel(item.key); setFocusedInput(null); }}>
          <span className="cursor-pointer select-text">{nl ? item.nl : item.en}{item.key === 'scenario' ? ` ${scenarioYear}` : ''}</span>
        </button>)}
    </nav>;
  return <div ref={root} className="space-y-2">
    <div className={valuationToolbar}>
      <label className={valuationField}>{t('Find company', 'Zoek bedrijf')} <input type="search" className={`${valuationControl} min-w-0`} value={search} disabled={panel === 'memberships' || panel === 'parameters'} onChange={event => setSearch(event.target.value)} /></label>
      <label className={valuationField}>{t('Sort', 'Sorteren')} <select className={valuationControl} value={columnSort ? 'column' : sort} disabled={panel === 'memberships' || panel === 'parameters'} onChange={event => { setColumnSort(null); setSort(event.target.value); }}>
        <option value="column" disabled>{t("Column sort", "Kolomsortering")}</option>
        <option value="name">{t('Company A–Z', 'Bedrijf A–Z')}</option>
        <option value="fairUpside">{t('Fair upside: highest first', 'Opwaarts potentieel: hoogste eerst')}</option>
        <option value="cagr">{t('Base CAGR: highest first', 'CAGR basis: hoogste eerst')}</option>
      </select></label>
      <span data-column="as_of" className={`${valuationControl} ${focusedInput === 'as_of' ? 'ring-2 ring-accent-400' : ''}`}>{t('As of', 'Peildatum')}: {workbook?.as_of ?? '—'}</span>
    </div>
    {tabs}
    <div ref={viewport} data-valuation-viewport className="relative h-[min(65vh,48rem)] min-h-80 overflow-auto rounded-xl border border-neutral-800/40 [scrollbar-gutter:stable]">
      {error && <p role="alert" className="absolute right-3 top-3 z-30 max-w-lg rounded-lg bg-card p-3 text-sm text-neg-300 shadow-lg">{error}</p>}
      {error && !workbook && <button type="button" className={control} onClick={() => { setError(''); setLoadAttempt(value => value + 1); }}>{t('Retry valuation inputs', 'Waarderingsinvoer opnieuw laden')}</button>}
      {!workbook && !error && <p role="status" className="absolute right-3 top-3 z-30 rounded-lg bg-card p-3 text-sm text-fg-muted">{t('Loading valuation inputs…', 'Waarderingsinvoer laden…')}</p>}
    {formula && <section aria-label={t('Formula details', 'Formuledetails')} style={formulaPosition} className={`absolute z-20 space-y-4 overflow-auto rounded-xl border p-6 text-sm shadow-xl ${COLUMN_STYLE.calculation}`}>
      <div className="flex items-center justify-between gap-3"><strong>{nl ? formula.nl : formula.en}</strong>
        <button type="button" onClick={() => setFormula(null)}>{t('Close formula', 'Formule sluiten')}</button></div>
      {workbook && <ValuationFormulaExplanation calculation={formula.key as CalculationKey} workbook={workbook}
        row={workbook.rows.find(row => row.isin === (formulaIsin || entries[0]?.quote.isin))} nl={nl}
        onInput={key => { setPanel(panelForInput(key)); setFocusedInput(key); setFormula(null); }} />}

    </section>}
    {panel === 'memberships' && <section aria-label={t('Portfolio abbreviations', 'Portefeuilleafkortingen')} className="space-y-3">
      <div><table className={`w-full text-left text-sm ${COLUMN_STYLE.data}`}>
        <thead><tr>{[t('Code', 'Code'), t('Portfolio / selection', 'Portefeuille / selectie'), t('Profiles', 'Profielen'), t('Source', 'Bron'), t('Holdings date', 'Positiedatum'), t('Last retrieved', 'Laatst opgehaald'), t('Coverage', 'Dekking')].map(label => <th key={label} className="sticky top-0 z-10 bg-card px-3 py-2">{label}</th>)}</tr></thead>
        <tbody>{workbook?.membership_legend?.map(item => <tr key={item.code} className="border-t border-neutral-800/30">
          <td className="px-3 py-2 font-semibold">{item.code}</td><td className="px-3 py-2">{item.name}</td>
          <td className="px-3 py-2">{item.portfolios.map(profile => <span key={profile.name} className="block">{profile.name}</span>)}</td>
          <td className="px-3 py-2">{item.source}</td>
          <td className="px-3 py-2">{item.portfolios.map(profile => <span key={profile.name} className="block">{profile.as_of ?? t('Date unavailable', 'Datum niet beschikbaar')}</span>)}</td>
          <td className="px-3 py-2">{item.portfolios.map(profile => <span key={profile.name} className="block whitespace-nowrap">{profile.retrieved_at && Number.isFinite(Date.parse(profile.retrieved_at))
            ? <time dateTime={profile.retrieved_at} title={profile.retrieved_at}>{new Date(profile.retrieved_at).toLocaleString(nl ? 'nl-NL' : 'en-GB', { dateStyle: 'short', timeStyle: 'short' })}</time>
            : t('Not recorded', 'Niet vastgelegd')}</span>)}</td>
          <td className="px-3 py-2" title={item.portfolios.filter(profile => !profile.available).map(profile => `${profile.name}: ${profile.reason ?? t('Holdings unavailable', 'Posities ontbreken')}`).join('\n')}>
            {item.status === 'complete' ? t('Complete', 'Volledig') : item.status === 'partial' ? t('Partial', 'Gedeeltelijk') : t('Unavailable', 'Niet beschikbaar')}
          </td>
        </tr>)}</tbody>
      </table></div>
      <p className="text-center text-sm text-fg-muted">{t('Codes show which portfolios hold each company, using saved AIRS and Leonteq holdings.',
        'Codes tonen welke portefeuilles elk bedrijf aanhouden, op basis van opgeslagen AIRS- en Leonteq-posities.')}</p>
    </section>}
    {panel === 'parameters' && workbook && <section className="space-y-3 p-4"><details ref={parametersForm} open className="rounded-lg border border-neutral-800/40 p-3">
      <summary className="cursor-pointer text-sm">{t('Portfolio valuation parameters', 'Parameters voor portefeuillewaardering')}</summary>
      <form className="mt-3 flex flex-wrap items-end gap-3" onSubmit={event => {
        event.preventDefault(); const form = new FormData(event.currentTarget);
        const parameters = { ...workbook.parameters };
        for (const key of Object.keys(parameters) as (keyof typeof parameters)[]) {
          if (form.has(key)) parameters[key] = numeric(form, key, key !== 'years')!;
        }
        void save(parameters);
      }}>
        {(Object.entries(workbook.parameters) as [keyof Workbook['parameters'], number][]).map(([key, val]) => {
          const labels = { years: t('Years', 'Jaren'), safety_margin: t('Safety margin (%)', 'Veiligheidsmarge (%)'),
            bear_growth_delta: t('Bear growth change (pp)', 'Negatief groei (pp)'), bear_multiple_delta: t('Bear multiple change (%)', 'Negatief multiple (%)'),
            bull_growth_delta: t('Bull growth change (pp)', 'Positief groei (pp)'), bull_multiple_delta: t('Bull multiple change (%)', 'Positief multiple (%)') };
          return <label key={`${key}:${val}`} className={`space-y-1 rounded-lg p-2 text-xs ${COLUMN_STYLE.assumption}`}><span className="block">{labels[key]}</span>
            <input className={`${control} w-28`} type="number" name={key} step={key === 'years' ? '1' : 'any'} required disabled={readOnly}
              min={key === 'years' ? 1 : key === 'safety_margin' ? 0 : undefined} max={key === 'years' ? 30 : key === 'safety_margin' ? 99.999 : undefined}
              defaultValue={val * (key === 'years' ? 1 : 100)} /></label>;
        })}
        {!readOnly && <button disabled={busy} className={control}>{t('Save assumptions', 'Aannames opslaan')}</button>}
      </form>
    </details></section>}
    {panel !== 'memberships' && panel !== 'parameters' && <>
    <div ref={table}><table className="w-full whitespace-nowrap text-xs">
      <caption className="sr-only">{t('Workbook valuation by company', 'Waardering per bedrijf volgens werkmap')}</caption>
      <thead className="text-xs text-fg-muted"><tr>{columns.map(column => <th key={column.key} data-column={column.key} data-kind={column.kind}
        aria-sort={(columnSort?.key ?? (sort === 'name' ? 'company' : sort)) === column.key
          ? ((columnSort?.descending ?? sort !== 'name') ? 'descending' : 'ascending') : undefined} className={`sticky top-0 z-10 h-12 px-2 py-1.5 text-left ${COLUMN_STYLE[column.kind]} ${panel === 'company' || panel === 'valuation' || panel === 'scenario' ? 'max-w-36 whitespace-normal' : ''} ${column.key === 'company' ? 'left-0 z-[15] w-44 min-w-44 max-w-44' : ''} ${focusedInput === column.key ? 'ring-2 ring-inset ring-accent-400' : ''}`}>
          <button type="button" className="cursor-pointer text-left hover:text-accent-300" onClick={() => {
            setFormula(null);
            setColumnSort({ key: column.key, descending: columnSort?.key === column.key && !columnSort.descending });
          }}>{nl ? column.nl : column.en}{columnSort?.key === column.key && <span aria-hidden="true"> {columnSort.descending ? '↓' : '↑'}</span>}</button>
          {column.key === 'selections' && membershipGaps.length > 0 && <button type="button" className="mt-1 block text-xs font-normal underline" title={coverageNote}
            onClick={() => { setPanel('memberships'); setFocusedInput(null); }}>{t('Coverage details', 'Dekkingsdetails')}</button>}
        </th>)}</tr></thead>
      <tbody className="divide-y divide-neutral-800/30">{entries.map(({ quote, row, result, cells }, i) => {
        const source = row?.quote ?? {};
        const percentages = new Set(['growth', 'safety_margin', 'fairUpside', 'buyUpside', 'cagr']);
        const multiples = new Set(['fair_multiple', 'exit_multiple', 'currentMultiple', 'buyMultiple']);
        return <tr key={quote.isin ?? `${quote.name}:${i}`} aria-busy={refreshingIsin === quote.isin} className={`h-12 ${refreshingIsin === quote.isin ? 'opacity-40 grayscale pointer-events-none' : ''}`}>
          {columns.map(column => {
            const raw = cells[column.key];
            const cagrStyle = column.key === 'cagr' && typeof raw === 'number' && Number.isFinite(raw)
              ? raw > 10 ? 'text-pos-400' : raw >= 7 ? 'text-orange-400' : 'text-neg-400'
              : '';
            const fairUpsideStyle = column.key === 'fairUpside' && typeof raw === 'number' && Number.isFinite(raw)
              ? raw < 0 ? 'text-neg-400' : 'text-pos-400'
              : '';
            const display = typeof raw === 'number' ? number(column.key === 'growth' || column.key === 'safety_margin' ? raw * 100 : raw,
              percentages.has(column.key) ? '%' : multiples.has(column.key) ? '×' : '') : raw || '—';
            return <td key={column.key} data-column={column.key} data-kind={column.kind}
              title={column.formula ? result?.reason ?? t('Click to explain this value', 'Klik voor uitleg van deze waarde') : raw == null ? undefined : String(raw)}
              className={`px-2 py-1.5 tabular-nums ${COLUMN_STYLE[column.kind]} ${column.key === 'company' ? 'sticky left-0 w-44 min-w-44 max-w-44 bg-card font-medium' : ''} ${column.key === 'risk_note' ? 'max-w-80 whitespace-normal' : ''}`}>
              {row && assumptionCell(column, row, quote.name) || (column.key === 'selections' ? <div className="flex max-h-16 flex-wrap items-center gap-1 overflow-y-auto">
                {row?.selections.map(code => <span key={code} className="rounded border border-neutral-800/40 px-1.5 py-0.5"
                  title={`${code} = ${workbook?.membership_legend?.find(item => item.code === code)?.name ?? code}${row.membership_profiles?.[code]?.length ? `: ${row.membership_profiles[code].join(', ')}` : ''}`}>{code}</span>)}
                {!row?.selections.length && <span title={row?.membership_unknown_codes?.join(', ')}>{row?.membership_complete === false ? t('Not confirmed', 'Niet bevestigd') : '—'}</span>}
              </div> : column.key === 'company' ? <><span className="block max-w-full truncate" title={quote.name}>{quote.name}</span>{row?.forecast.basis_label?.includes('ordinary-share reference') && <span className="block text-xs font-normal text-neutral-500">{t('Ordinary-share reference', 'Referentie: gewoon aandeel')}</span>}{refreshingIsin === quote.isin && <span role="status" aria-label={t('Refreshing row', 'Rij vernieuwen')} className="ml-2 inline-block h-3 w-3 animate-spin rounded-full border-2 border-current border-t-transparent" />}</>
                  : (column.key === 'raw_price' || column.key === 'price') && typeof raw === 'number' && source.symbol
                    ? <a href={`https://finance.yahoo.com/quote/${encodeURIComponent(source.symbol)}/`} target="_blank" rel="noopener noreferrer"
                        className="cursor-pointer underline decoration-current/40 underline-offset-2 hover:text-accent-300"
                        title={t(`Open ${source.symbol} on Yahoo Finance`, `Open ${source.symbol} op Yahoo Finance`)}>{display}</a>
                  : column.key === 'risk_note' ? <span className="line-clamp-2 max-w-80 whitespace-normal">{display}</span>
                  : column.formula ? <button type="button" className={`cursor-pointer text-left ${column.key === 'signal'
                    ? `rounded-md px-2 py-1 font-medium ${SIGNAL_STYLE[String(result?.signal)] ?? SIGNAL_STYLE.REVIEW}`
                    : `underline decoration-dotted underline-offset-4 ${cagrStyle} ${fairUpsideStyle}`}`}
                      aria-label={t(`Explain ${column.en} for ${quote.name}`, `Uitleg ${column.nl} voor ${quote.name}`)}
                      onClick={event => openFormula(column, quote.isin ?? '', event.currentTarget)}>{display}</button> : display)}
            </td>;
          })}
        </tr>;
      })}{!entries.length && <tr><td colSpan={columns.length} className="px-3 py-6 text-center text-fg-muted">{rows.length
        ? t('No matching companies.', 'Geen overeenkomende bedrijven.')
        : t('Press Refresh to load companies.', 'Klik op Vernieuwen om bedrijven te laden.')}</td></tr>}</tbody>
    </table></div></>}
    {panel === 'sources' && entries.some(entry => entry.warning) && <section aria-label={t('Data checks', 'Gegevenscontroles')} className="space-y-2 text-center text-xs text-fg-muted">
      <h3 className="font-medium">{t('Data checks', 'Gegevenscontroles')}</h3>
      {entries.filter(entry => entry.warning).map(({ quote, warning }, index) => <p key={quote.isin ?? index}><strong>{quote.name}</strong>: {warning}</p>)}
    </section>}
    </div>
  </div>;
}
