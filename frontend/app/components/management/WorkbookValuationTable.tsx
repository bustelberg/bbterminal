'use client';

import { useCallback, useEffect, useRef, useState, type FormEvent } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import PanelDialog from '../portfolios/PanelDialog';
import { workbookValuation, type CalculationKey, type Workbook, type WorkbookRow, type CompanyAssumption } from './valuationWorkbook';
import { COLUMN_STYLE, WORKBOOK_PANELS, columnsForPanel, panelForInput, type WorkbookColumn, type WorkbookPanelKey } from './valuationWorkbookColumns';
import ValuationFormulaExplanation from './ValuationFormulaExplanation';
import { selectingTabText } from './valuationTabSelection';

type Quote = { name: string; isin?: string | null; price?: number | null; price_date?: string | null; currency?: string | null };
const control = 'rounded-lg border border-neutral-700 bg-elevated px-3 py-2 text-sm';
const sourceInputs = new Set(['manual_basis', 'basis_currency', 'basis_label', 'basis_date', 'basis_source']);
const sourceFields = (row?: WorkbookRow) => row ? {
  ticker: row.ticker, quote: row.quote, forecast: row.forecast,
  assumptions: row.assumptions, seed_source: row.seed_source,
} : {};
const number = (v: number | null | undefined, suffix = '') => v == null || !Number.isFinite(v) ? '—' : `${v.toFixed(2)}${suffix}`;

export default function WorkbookValuationTable({ portfolioId, rows, nl, portfolioName, refreshingIsin = null, workbookUpdate = null, workbookData }: {
  portfolioId: number; rows: Quote[]; nl: boolean; portfolioName: string; refreshingIsin?: string | null; workbookUpdate?: Workbook | null; workbookData?: Workbook | null;
}) {
  const [workbook, setWorkbook] = useState<Workbook | null>(workbookData ?? null);
  const hasWorkbook = workbook !== null;
  const [error, setError] = useState('');
  const [loadAttempt, setLoadAttempt] = useState(0);
  const refreshedRows = useRef(new Map<string, WorkbookRow>());
  const [busy, setBusy] = useState(false);
  const [editing, setEditing] = useState<WorkbookRow | null>(null);
  const [panel, setPanel] = useState<WorkbookPanelKey>('company');
  const [search, setSearch] = useState('');
  const [sort, setSort] = useState('name');
  const [formulaIsin, setFormulaIsin] = useState('');
  const [formula, setFormula] = useState<WorkbookColumn | null>(null);
  const [focusedInput, setFocusedInput] = useState<string | null>(null);
  const table = useRef<HTMLDivElement>(null);
  const viewport = useRef<HTMLDivElement>(null);
  const root = useRef<HTMLDivElement>(null);
  const parametersForm = useRef<HTMLDetailsElement>(null);
  const alive = useRef(true);
  const t = useCallback((en: string, dutch: string) => nl ? dutch : en, [nl]);
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
      : result?.reason ?? (result?.signal === 'REVIEW' ? t('Complete valuation assumptions', 'Vul de waarderingsaannames aan')
        : !row ? t('Valuation inputs unavailable', 'Waarderingsinvoer ontbreekt')
          : result?.cagr == null ? t('Scenario assumptions incomplete', 'Scenarioaannames onvolledig') : null);
    return { quote, row, result, warning };
  }).filter(({ quote, row }) => `${quote.name} ${row?.ticker ?? ''}`.toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => sort === 'name' ? a.quote.name.localeCompare(b.quote.name)
      : ((b.result?.[sort as 'fairUpside' | 'cagr'] ?? -Infinity) - (a.result?.[sort as 'fairUpside' | 'cagr'] ?? -Infinity)
        || a.quote.name.localeCompare(b.quote.name)));

  const save = async (parameters: Workbook['parameters'], changed?: WorkbookRow) => {
    if (!workbook || busy) return;
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
        setEditing(null);
      }
    } catch (cause) { if (alive.current) setError(String(cause instanceof Error ? cause.message : cause)); }
    finally { if (alive.current) setBusy(false); }
  };
  const numeric = (form: FormData, key: string, percent = false) => {
    const raw = String(form.get(key) ?? '').trim();
    return raw === '' ? null : Number(raw) / (percent ? 100 : 1);
  };
  const updateCompany = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!editing || !workbook) return;
    const form = new FormData(event.currentTarget);
    const assumptions: CompanyAssumption = { ...editing.assumptions,
      growth: numeric(form, 'growth', true), fair_multiple: numeric(form, 'fair_multiple'),
      exit_multiple: numeric(form, 'exit_multiple'), manual_basis: numeric(form, 'manual_basis'),
      basis_fx: numeric(form, 'basis_fx'), months_fy1: numeric(form, 'months_fy1'),
      basis_kind: form.get('basis_kind') === 'manual' ? 'manual' : 'forward_eps',
      basis_label: String(form.get('basis_label') ?? ''), basis_currency: String(form.get('basis_currency') ?? '') || null,
      basis_date: String(form.get('basis_date') ?? '') || null, basis_source: String(form.get('basis_source') ?? '') || null,
      risk_note: String(form.get('risk_note') ?? ''), review_required: form.has('review_required') };
    void save(workbook.parameters, { ...editing, assumptions });
  };
  const numericField = (key: keyof CompanyAssumption, label: string, percent = false) => <label className={`space-y-1 rounded-lg p-2 text-sm ${COLUMN_STYLE[sourceInputs.has(key) ? 'data' : 'assumption']}`} key={key}>
    <span className="block">{label}</span><input name={key} type="number" step="any" className={`${control} w-full`}
      defaultValue={typeof editing?.assumptions[key] === 'number' ? Number(editing.assumptions[key]) * (percent ? 100 : 1) : ''}
      min={key === 'growth' ? -99.999 : key === 'months_fy1' ? 0 : key === 'manual_basis' ? undefined : .000001}
      max={key === 'months_fy1' ? 12 : undefined} /></label>;
  const textField = (key: keyof CompanyAssumption, label: string) => <label className={`space-y-1 rounded-lg p-2 text-sm ${COLUMN_STYLE[sourceInputs.has(key) ? 'data' : 'assumption']}`} key={key}>
    <span className="block">{label}</span><input name={key} className={`${control} w-full`} defaultValue={String(editing?.assumptions[key] ?? '')} /></label>;
  const tabs = <nav className="flex flex-wrap gap-2" aria-label={t('Valuation panels', 'Waarderingspanelen')}>
      {WORKBOOK_PANELS.map(item => <button key={item.key} type="button" aria-pressed={panel === item.key}
        className={`${control} cursor-pointer select-text ${item.key === 'parameters' ? 'ml-auto' : ''} aria-pressed:border-accent-500/50 aria-pressed:text-accent-300`}
        onClick={event => { if (selectingTabText(event)) return; setPanel(item.key); setFocusedInput(null); }}>
          <span className="cursor-pointer select-text">{nl ? item.nl : item.en}{item.key === 'scenario' ? ` ${scenarioYear}` : ''}</span>
        </button>)}
    </nav>;
  return <div ref={root} className="space-y-3">
    <div className="flex flex-wrap items-center gap-3">
      <label className="text-xs text-fg-muted">{t('Find company', 'Zoek bedrijf')} <input type="search" className={control} value={search} disabled={panel === 'memberships' || panel === 'parameters'} onChange={event => setSearch(event.target.value)} /></label>
      <label className="text-xs text-fg-muted">{t('Sort', 'Sorteren')} <select className={control} value={sort} disabled={panel === 'memberships' || panel === 'parameters'} onChange={event => setSort(event.target.value)}>
        <option value="name">{t('Company A–Z', 'Bedrijf A–Z')}</option>
        <option value="fairUpside">{t('Fair upside: highest first', 'Opwaarts potentieel: hoogste eerst')}</option>
        <option value="cagr">{t('Base CAGR: highest first', 'CAGR basis: hoogste eerst')}</option>
      </select></label>
    <div className="text-xs">
      <span data-column="as_of" className={`${COLUMN_STYLE.data} rounded px-2 py-1 ${focusedInput === 'as_of' ? 'ring-2 ring-accent-400' : ''}`}>{t('As of', 'Peildatum')}: {workbook?.as_of ?? '—'}</span>
    </div>
    </div>
    <p className="text-xs text-fg-subtle">{t('Click a yellow heading to inspect its formula and open the blue/green inputs. Click a company to edit its assumptions.',
      'Klik op een gele kolomkop voor de formule en de blauwe/groene invoer. Klik op een bedrijf om aannames te wijzigen.')}</p>
    {tabs}
    <div ref={viewport} data-valuation-viewport className="relative h-[min(65vh,48rem)] min-h-80 overflow-auto rounded-xl border border-neutral-800/40 [scrollbar-gutter:stable]">
      {error && <p role="alert" className="absolute right-3 top-3 z-30 max-w-lg rounded-lg bg-card p-3 text-sm text-neg-300 shadow-lg">{error}</p>}
      {error && !workbook && <button type="button" className={control} onClick={() => { setError(''); setLoadAttempt(value => value + 1); }}>{t('Retry valuation inputs', 'Waarderingsinvoer opnieuw laden')}</button>}
      {!workbook && !error && <p role="status" className="absolute right-3 top-3 z-30 rounded-lg bg-card p-3 text-sm text-fg-muted">{t('Loading valuation inputs…', 'Waarderingsinvoer laden…')}</p>}
    {formula && <section aria-label={t('Formula details', 'Formuledetails')} style={{ backgroundColor: 'color-mix(in srgb, var(--color-card) 90%, #eab308)' }} className={`absolute inset-x-3 top-3 z-20 max-h-[calc(100%_-_1.5rem)] space-y-2 overflow-auto rounded-lg border p-3 text-xs shadow-xl ${COLUMN_STYLE.calculation}`}>
      <div className="flex items-center justify-between gap-3"><strong>{nl ? formula.nl : formula.en}</strong>
        <button type="button" onClick={() => setFormula(null)}>{t('Close formula', 'Formule sluiten')}</button></div>
      <label className="block space-y-1 text-sm"><span>{t('Company example', 'Voorbeeldbedrijf')}</span>
        <select className={control} value={formulaIsin || entries[0]?.quote.isin || ''} onChange={event => setFormulaIsin(event.target.value)}>
          {entries.filter(entry => entry.quote.isin).map(entry => <option key={entry.quote.isin} value={entry.quote.isin!}>{entry.quote.name}</option>)}
        </select>
      </label>
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
      <p className="text-sm text-fg-muted">{t('Codes show membership in the latest saved holdings, matched by exact ISIN. BB combines the standard Bustelberg profiles; BB2 is separate. A code means the company is present in at least one listed profile. Linked certificate holdings are included when available.',
        'Codes tonen lidmaatschap op basis van de laatst opgeslagen posities, gekoppeld op exact ISIN. BB combineert de standaard Bustelberg-profielen; BB2 staat apart. Een code betekent dat het bedrijf in minstens één genoemd profiel voorkomt. Posities via gekoppelde certificaten tellen mee indien beschikbaar.')}</p>
      <p className="text-xs text-fg-subtle">{t('Holdings date is the date reported by the source. AIRS may retain an older effective model date after a successful refresh. Last retrieved is when we fetched the snapshot. Complete describes holdings coverage; it does not confirm freshness or that a holdings date is known.',
        'Positiedatum is de datum die de bron rapporteert. AIRS kan na succesvol vernieuwen een oudere ingangsdatum van het model behouden. Laatst opgehaald is wanneer wij de momentopname hebben opgehaald. Volledig beschrijft de positiedekking; het bevestigt niet dat de gegevens actueel zijn of dat de positiedatum bekend is.')}</p>
      {membershipGaps.length > 0 && <p role="status" className="text-xs text-fg-muted">{coverageNote}</p>}
      {!workbook?.membership_legend?.length && <p className="text-sm text-fg-muted">{t('Membership definitions are not available yet.', 'De lidmaatschapsdefinities zijn nog niet beschikbaar.')}</p>}
      <p className="text-xs text-fg-subtle">{t('Topselecties use Leonteq only. Other portfolios use AIRS. Missing holdings make membership coverage incomplete; an absent code then does not confirm non-membership. Excel membership flags are not used.',
        'Topselecties gebruiken uitsluitend Leonteq. Overige portefeuilles gebruiken AIRS. Ontbrekende posities maken de dekking onvolledig; een ontbrekende code bewijst dan geen afwezigheid. Excel-lidmaatschapsvlaggen worden niet gebruikt.')}</p>
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
            <input className={`${control} w-28`} type="number" name={key} step={key === 'years' ? '1' : 'any'} required
              min={key === 'years' ? 1 : key === 'safety_margin' ? 0 : undefined} max={key === 'years' ? 30 : key === 'safety_margin' ? 99.999 : undefined}
              defaultValue={val * (key === 'years' ? 1 : 100)} /></label>;
        })}
        <button disabled={busy} className={control}>{t('Save assumptions', 'Aannames opslaan')}</button>
      </form>
    </details></section>}
    {panel !== 'memberships' && panel !== 'parameters' && <>
    <div ref={table}><table className="w-full whitespace-nowrap text-sm">
      <caption className="sr-only">{t('Workbook valuation by company', 'Waardering per bedrijf volgens werkmap')}</caption>
      <thead className="bg-elevated text-xs text-fg-muted"><tr>{columns.map(column => <th key={column.key} data-column={column.key} data-kind={column.kind}
        style={{ backgroundColor: `color-mix(in srgb, var(--color-card) 90%, ${column.kind === 'data' ? '#3b82f6' : column.kind === 'assumption' ? '#22c55e' : '#eab308'})` }}
        title={column.formula ? t('Click to see the calculation and company inputs', 'Klik voor de berekening en bedrijfsinvoer') : undefined} className={`sticky top-0 z-10 h-16 border-t-2 px-3 py-2 text-left ${COLUMN_STYLE[column.kind]} ${panel === 'company' || panel === 'valuation' || panel === 'scenario' ? 'max-w-36 whitespace-normal' : ''} ${column.key === 'company' ? 'left-0 z-[15] w-56 min-w-56 max-w-56' : ''} ${focusedInput === column.key ? 'ring-2 ring-inset ring-accent-400' : ''}`}>
          {column.formula ? <button type="button" className="text-left underline decoration-dotted underline-offset-4" onClick={() => { setFormulaIsin(entries[0]?.quote.isin ?? ''); setFormula(column); }}>{nl ? column.nl : column.en}</button> : nl ? column.nl : column.en}
          {column.key === 'selections' && membershipGaps.length > 0 && <button type="button" className="mt-1 block text-xs font-normal underline" title={coverageNote}
            onClick={() => { setPanel('memberships'); setFocusedInput(null); }}>{t('Coverage details', 'Dekkingsdetails')}</button>}
        </th>)}</tr></thead>
      <tbody className="divide-y divide-neutral-800/30">{entries.map(({ quote, row, result, warning }, i) => {
        const source = row?.quote ?? {};
        const a = row?.assumptions, f = row?.forecast;
        const signals = nl ? { REVIEW: 'NADER TOETSEN', BUY: 'KOOP', CONSIDER: 'TWIJFEL', 'DO NOT BUY': 'NIET KOPEN' }
          : { REVIEW: 'REVIEW', BUY: 'BUY', CONSIDER: 'CONSIDER', 'DO NOT BUY': 'DO NOT BUY' };
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
          ...result, signal: signals[result?.signal as keyof typeof signals ?? 'REVIEW'],
        };
        const percentages = new Set(['growth', 'safety_margin', 'fairUpside', 'buyUpside', 'cagr']);
        const multiples = new Set(['fair_multiple', 'exit_multiple', 'currentMultiple', 'buyMultiple']);
        return <tr key={quote.isin ?? `${quote.name}:${i}`} aria-busy={refreshingIsin === quote.isin} className={`h-20 ${refreshingIsin === quote.isin ? 'opacity-40 grayscale pointer-events-none' : ''}`}>
          {columns.map(column => {
            const raw = cells[column.key];
            const display = typeof raw === 'number' ? number(column.key === 'growth' || column.key === 'safety_margin' ? raw * 100 : raw,
              percentages.has(column.key) ? '%' : multiples.has(column.key) ? '×' : '') : raw || '—';
            return <td key={column.key} data-column={column.key} data-kind={column.kind}
              style={column.key === 'company' ? { backgroundColor: 'color-mix(in srgb, var(--color-card) 90%, #3b82f6)' } : undefined}
              title={column.formula ? result?.reason ?? t('Click to explain this value', 'Klik voor uitleg van deze waarde') : raw == null ? undefined : String(raw)}
              className={`px-3 py-2 tabular-nums ${COLUMN_STYLE[column.kind]} ${column.key === 'company' ? 'sticky left-0 w-56 min-w-56 max-w-56 bg-card font-medium' : ''} ${column.key === 'risk_note' ? 'max-w-80 whitespace-normal' : ''}`}>
              {column.key === 'selections' ? <div className="flex max-h-16 flex-wrap items-center gap-1 overflow-y-auto">
                {row?.selections.map(code => <span key={code} className="rounded border border-blue-500/30 px-1.5 py-0.5"
                  title={`${code} = ${workbook?.membership_legend?.find(item => item.code === code)?.name ?? code}${row.membership_profiles?.[code]?.length ? `: ${row.membership_profiles[code].join(', ')}` : ''}`}>{code}</span>)}
                {!row?.selections.length && <span title={row?.membership_unknown_codes?.join(', ')}>{row?.membership_complete === false ? t('Not confirmed', 'Niet bevestigd') : '—'}</span>}
              </div> : column.key === 'company' ? <><button disabled={!row || !quote.isin || busy} className="block max-w-full truncate text-left hover:text-accent-300 disabled:text-fg-muted"
                onClick={() => setEditing(row ?? null)} title={`${quote.name} · ${t('Edit assumptions', 'Aannames wijzigen')}`}>{quote.name}</button>{refreshingIsin === quote.isin && <span role="status" aria-label={t('Refreshing row', 'Rij vernieuwen')} className="ml-2 inline-block h-3 w-3 animate-spin rounded-full border-2 border-current border-t-transparent" />}</>
                  : (column.key === 'raw_price' || column.key === 'price') && typeof raw === 'number' && source.symbol
                    ? <a href={`https://finance.yahoo.com/quote/${encodeURIComponent(source.symbol)}/`} target="_blank" rel="noopener noreferrer"
                        className="cursor-pointer underline decoration-current/40 underline-offset-2 hover:text-accent-300"
                        title={t(`Open ${source.symbol} on Yahoo Finance`, `Open ${source.symbol} op Yahoo Finance`)}>{display}</a>
                  : column.key === 'risk_note' ? <span className="line-clamp-2 max-w-80 whitespace-normal">{display}</span>
                  : column.formula ? <button type="button" className="cursor-pointer text-left underline decoration-dotted underline-offset-4"
                      aria-label={t(`Explain ${column.en} for ${quote.name}`, `Uitleg ${column.nl} voor ${quote.name}`)}
                      onClick={() => { setFormulaIsin(quote.isin ?? ''); setFormula(column); }}>{display}</button> : display}
              {column.key === 'signal' && warning && <button type="button" className="mt-1 block max-w-40 whitespace-normal text-left text-xs underline" title={warning}
                onClick={() => { setPanel('sources'); setFocusedInput(null); }}>{t('Check inputs', 'Controleer invoer')}</button>}
            </td>;
          })}
        </tr>;
      })}{!entries.length && <tr><td colSpan={columns.length} className="px-3 py-6 text-fg-muted">{t('No matching companies.', 'Geen overeenkomende bedrijven.')}</td></tr>}</tbody>
    </table></div></>}
    {panel === 'sources' && <p className="text-xs text-fg-subtle">{t('Prices: saved Yahoo closes. Forecasts: saved GuruFocus consensus. Portfolio codes come from saved holdings. Their definitions and coverage are in Afkortingen.',
      'Koersen: opgeslagen Yahoo-slotkoersen. Prognoses: opgeslagen GuruFocus-consensus. Portefeuillecodes komen uit opgeslagen posities. Definities en dekking staan onder Afkortingen.')}</p>}
    {panel === 'sources' && <section aria-label={t('Data checks', 'Gegevenscontroles')} className="space-y-2 text-xs text-fg-muted">
      <h3 className="font-medium">{t('Data checks', 'Gegevenscontroles')}</h3>
      {entries.filter(entry => entry.warning).map(({ quote, warning }, index) => <p key={quote.isin ?? index}><strong>{quote.name}</strong>: {warning}</p>)}
      {!entries.some(entry => entry.warning) && <p>{t('No valuation input warnings for the displayed companies.', 'Geen waarschuwingen voor de waarderingsinvoer van de getoonde bedrijven.')}</p>}
    </section>}
    </div>
    {editing && <PanelDialog onClose={() => { if (!busy) setEditing(null); }} labelledBy="valuation-assumptions-title">
      <form onSubmit={updateCompany} className="min-h-0 flex-1 space-y-4 overflow-auto rounded-xl border border-neutral-800/40 bg-card p-5">
        <h2 id="valuation-assumptions-title" className="text-lg font-semibold">{rows.find(row => row.isin === editing.isin)?.name} · {t('Assumptions', 'Aannames')}</h2>
        <p className="text-xs text-fg-subtle">{portfolioName} · {editing.seed_source ? t('Starting values from test.xlsx; forecasts and prices refresh separately.', 'Startwaarden uit test.xlsx; prognoses en koersen worden afzonderlijk bijgewerkt.') : t('Enter your analyst assumptions.', 'Vul uw analistenaannames in.')}</p>
        <div className="grid gap-3 sm:grid-cols-3">
          {numericField('growth', t('Base growth p.a. (%)', 'Groei basis p.j. (%)'), true)}
          {numericField('fair_multiple', t('Fair multiple', 'Faire multiple'))}{numericField('exit_multiple', t('Exit multiple', 'Exitmultiple'))}
          <label className={`space-y-1 rounded-lg p-2 text-sm ${COLUMN_STYLE.assumption}`}><span className="block">{t('Basis', 'Waarderingsbasis')}</span><select name="basis_kind" defaultValue={editing.assumptions.basis_kind ?? 'forward_eps'} className={`${control} w-full`}>
            <option value="forward_eps">GuruFocus NTM EPS</option><option value="manual">{t('Manual NAV / BV / FCF / EPS', 'Handmatig NAV / BV / FCF / EPS')}</option></select></label>
          {numericField('manual_basis', t('Manual source value per share', 'Handmatige bronwaarde per aandeel'))}
          {textField('basis_currency', t('Manual basis currency', 'Valuta handmatige basis'))}
          {textField('basis_label', t('Manual basis label', 'Naam handmatige basis'))}
          {textField('basis_date', t('Manual basis date', 'Peildatum handmatige basis'))}
          {textField('basis_source', t('Manual source URL', 'Handmatige bron-URL'))}
          {numericField('basis_fx', t('Basis currency → quote currency FX', 'FX basisvaluta → koersvaluta'))}
          {numericField('months_fy1', t('FY1 months (blank = calendar)', 'Maanden FY1 (leeg = kalender)'))}
        </div>
        <p className={`rounded-lg p-2 text-xs ${COLUMN_STYLE.data}`}>{editing.forecast.fy1_date}: {number(editing.forecast.fy1)} · {editing.forecast.fy2_date}: {number(editing.forecast.fy2)} {editing.forecast.currency} · {editing.forecast.basis_label ?? editing.forecast.reason}</p>
        <label className={`block space-y-1 rounded-lg p-2 text-sm ${COLUMN_STYLE.assumption}`}><span>{t('Key risk / consideration', 'Kernrisico / aandachtspunt')}</span><textarea name="risk_note" className={`${control} w-full`} maxLength={2000} defaultValue={editing.assumptions.risk_note} /></label>
        <label className={`flex items-center gap-2 rounded-lg p-2 text-sm ${COLUMN_STYLE.assumption}`}><input name="review_required" type="checkbox" defaultChecked={editing.assumptions.review_required} />{t('Require further review before a buy signal', 'Nader toetsen voordat een koopsignaal wordt getoond')}</label>
        {error && <p role="alert" className="text-sm text-neg-300">{error}</p>}
        <div className="flex gap-3"><button type="submit" className={control} disabled={busy}>{busy ? t('Saving…', 'Opslaan…') : t('Save', 'Opslaan')}</button>
          <button type="button" className={control} disabled={busy} onClick={() => setEditing(null)}>{t('Cancel', 'Annuleren')}</button></div>
      </form>
    </PanelDialog>}
  </div>;
}
