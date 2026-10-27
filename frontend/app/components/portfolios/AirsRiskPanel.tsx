'use client';

import { useState, type ComponentProps } from 'react';
import dynamic from 'next/dynamic';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { useLang } from '../../../lib/i18n';
import type { components } from '../../../lib/api-types';
import type { ModelPortfolioAnalysis } from '../../../lib/types/api';
import type ActiveSharePanelType from './ActiveSharePanel';
import { airsRiskExposures, type Exposure } from './airsRiskExposures';
import { useRiskResult } from './useRiskResult';

// The simulation is not mounted (and issues no benchmark/history requests) until selected.
const CurrentPortfolioSimulation = dynamic(() => import('./ActiveSharePanel'), { ssr: false });
type History = components['schemas']['AirsRiskHistory'];
type Props = ComponentProps<typeof ActiveSharePanelType> & {
  airsHoldings: NonNullable<ModelPortfolioAnalysis['book_holdings']>;
  bands?: ModelPortfolioAnalysis['bands'];
};
type View = 'overview' | 'exposures' | 'losses' | 'simulation';
const percent = (value?: number | null) => value == null ? '—' : `${value.toFixed(2)}%`;
const day = (value?: string | null) => value ? new Date(`${value}T12:00:00`).toLocaleDateString('nl-NL', { day: 'numeric', month: 'short', year: 'numeric' }) : '—';

function historyWarning(warning: string, nl: boolean): string {
  if (!nl) return warning;
  if (warning.startsWith('January is missing')) return 'Januari ontbreekt; de daling wordt vanaf de eerste opgeslagen waarneming gemeten, niet vanaf jaarbegin.';
  if (warning.startsWith('There are gaps')) return 'Er zitten gaten van meer dan 35 dagen in de waarnemingen. Ontbrekende rendementen worden niet geïnterpoleerd.';
  if (warning.startsWith('At least two')) return 'Minimaal twee waargenomen datums nodig om een daling te rapporteren.';
  if (warning.startsWith('The latest observation')) return 'De laatste waarneming betreft een onvolledige maand en telt niet mee in statistieken over volledige maanden.';
  if (/^\d+ invalid/.test(warning)) return `${warning.split(' ')[0]} ongeldige of ontbrekende rendementswaarneming(en) uitgesloten.`;
  return warning;
}

function Metric({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return <div className="min-w-0 rounded-lg border border-neutral-800/40 bg-elevated p-3">
    <p className="text-xs text-fg-muted">{label}</p><p className="mt-1 font-mono text-xl text-fg-strong">{value}</p>
    {detail && <p className="mt-1 text-xs text-fg-faint">{detail}</p>}
  </div>;
}

function ExposureTable({ title, rows, note }: { title: string; rows: Exposure[]; note?: string }) {
  return <section className="min-w-0 rounded-lg border border-neutral-800/40 p-3">
    <h3 className="text-sm font-semibold text-fg-strong">{title}</h3>
    {note && <p className="mt-1 text-xs text-fg-faint">{note}</p>}
    {!rows.length ? <p className="mt-3 text-xs text-fg-muted">—</p> : <table className="mt-2 w-full text-xs"><tbody>
      {rows.map((row) => <tr key={row.name} className="border-t border-neutral-800/30">
        <td className="py-2 pr-2 text-fg-soft">{row.name}{row.routes.length > 0 && <span className="mt-0.5 block text-[11px] text-fg-faint">{row.routes.join(' · ')}</span>}</td>
        <td className="py-2 text-right align-top font-mono text-fg">{percent(row.weight)}</td>
      </tr>)}
    </tbody></table>}
  </section>;
}

function ObservedDrawdownChart({ history, nl }: { history: History; nl: boolean }) {
  const points = history.points ?? [];
  if ((history.observations ?? 0) < 2 || points.length < 2) return null;
  const start = Date.parse(points[0].date);
  const span = Math.max(1, Date.parse(points[points.length - 1].date) - start);
  const floor = Math.min(-1, ...points.map((point) => point.drawdown_pct));
  const plot = points.map((point) => ({
    point, x: 48 + (Date.parse(point.date) - start) / span * 700,
    y: 15 + point.drawdown_pct / floor * 130,
  }));
  return <section className="rounded-lg border border-neutral-800/40 p-3">
    <h3 className="text-sm font-semibold text-fg-strong">{nl ? 'Waargenomen daling vanaf piek' : 'Observed drawdown from peak'}</h3>
    <svg viewBox="0 0 800 175" role="img" aria-label={nl ? 'AIRS-drawdown op opgeslagen datums' : 'AIRS drawdown on stored dates'} className="mt-2 w-full text-accent-400">
      <line x1="48" x2="748" y1="15" y2="15" stroke="currentColor" opacity="0.3" />
      <text x="0" y="18" fill="currentColor" fontSize="11">0%</text>
      <text x="0" y="145" fill="currentColor" fontSize="11">{floor.toFixed(1)}%</text>
      <polyline points={plot.map(({ x, y }) => `${x},${y}`).join(' ')} fill="none" stroke="currentColor" strokeWidth="2" strokeDasharray="4 3" />
      {plot.map(({ point, x, y }) => <circle key={point.date} cx={x} cy={y} r="3" fill="currentColor"><title>{day(point.date)} · {percent(point.drawdown_pct)}</title></circle>)}
      <text x="48" y="168" fill="currentColor" fontSize="11">{day(points[0].date)}</text>
      <text x="748" y="168" textAnchor="end" fill="currentColor" fontSize="11">{day(points[points.length - 1].date)}</text>
    </svg>
    <p className="text-xs text-fg-faint">{nl ? 'Stippellijnen verbinden alleen de opgeslagen waarnemingen; geen dagelijkse rendementsreeks.' : 'Dashed lines only connect stored observations; this is not a daily return series.'}</p>
  </section>;
}

function AccountHistory({ account, view }: { account?: string | null; view: 'overview' | 'losses' }) {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const url = `${API_URL}/api/airs/accounts/${encodeURIComponent(account ?? '')}/risk-history`;
  const { data, error } = useRiskResult<History>(url, async () => {
    if (!account) return { portefeuille: '', source: 'AIRS', observations: 0, warnings: ['No paired AIRS account.'] };
    const response = await apiFetch(url);
    const body = await response.json();
    if (!response.ok) throw new Error(body?.detail ?? `HTTP ${response.status}`);
    return body as History;
  }, 'airs-risk', 'AIRS account risk history unavailable');
  if (!account) return <p className="text-sm text-fg-muted">{nl ? 'Geen gekoppelde AIRS-rekening. Werkelijke verliezen zijn niet beschikbaar voor een modelmandje.' : 'No paired AIRS account. Actual losses are unavailable for a model basket.'}</p>;
  if (error) return <p role="alert" className="text-sm text-warn-300">{nl ? 'AIRS-rendementshistorie niet beschikbaar. Geen simulatie als vervanging getoond.' : 'AIRS return history unavailable. A simulation has not been substituted.'} {error}</p>;
  if (!data) return <p role="status" className="text-sm text-fg-muted">{nl ? 'AIRS-waarnemingen laden…' : 'Loading AIRS observations…'}</p>;
  if (!data.observations) return <p className="rounded-lg border border-neutral-800/40 p-3 text-sm text-fg-muted">{nl ? 'Geen opgeslagen AIRS-rendementswaarnemingen van een gefinancierde rekening. Werkelijke verliesstatistieken zijn onbekend, niet nul.' : 'No stored AIRS return observations for a funded account. Actual loss statistics are unknown, not zero.'}</p>;
  const points = data.points ?? [];
  const months = data.months ?? [];
  return <div className="space-y-4">
    <p className="text-xs text-fg-faint">AIRS · {account} · {data.year ?? '—'} · {day(data.first_date)} – {day(data.last_date)} · {data.observations ?? 0} {nl ? 'waarnemingen' : 'observations'}</p>
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
      <Metric label={nl ? 'Grootste waargenomen daling' : 'Largest observed drawdown'} value={percent(data.max_drawdown_pct)} detail={nl ? 'Binnen de beschikbare waarnemingen' : 'Within stored observations'} />
      <Metric label={nl ? 'Huidige waargenomen daling' : 'Current observed drawdown'} value={percent(data.current_drawdown_pct)} />
      <Metric label={nl ? 'Slechtste volledige maand' : 'Worst complete month'} value={percent(data.worst_month_pct)} detail={`${months.length} ${nl ? 'volledige maandrendementen' : 'complete monthly returns'}`} />
      <Metric label={nl ? 'Negatieve volledige maanden' : 'Negative complete months'} value={`${data.negative_months ?? 0} / ${months.length}`} />
    </div>
    <div className="rounded-lg border border-warn-500/20 bg-warn-500/5 p-3 text-xs text-fg-muted">
      {nl ? 'Alleen werkelijk opgeslagen AIRS-rendementen, geen terugberekening met huidige posities. Alleen het laatst opgeslagen kalenderjaar. Tussentijdse verliezen kunnen groter zijn. Ontbrekende maanden en de lopende maand tellen niet als volledige maand. Geen volatiliteitsschatting uit deze korte/onregelmatige reeks.' : 'Actual stored AIRS returns only, not today’s holdings carried backwards. Latest stored calendar year only. Losses between observations may be larger. Missing months and the current partial month are not counted as complete months. No volatility estimate from this short/irregular series.'}
      {(data.warnings?.length ?? 0) > 1 && <ul className="mt-2 list-disc space-y-1 pl-4">{data.warnings?.slice(1).map((warning) => <li key={warning}>{historyWarning(warning, nl)}</li>)}</ul>}
    </div>
    {view === 'losses' && <>
      <ObservedDrawdownChart history={data} nl={nl} />
      <div className="grid gap-3 sm:grid-cols-3">
        <Metric label={nl ? 'Piek vóór grootste daling' : 'Peak before largest loss'} value={day(data.peak_date)} />
        <Metric label={nl ? 'Dieptepunt' : 'Trough'} value={day(data.trough_date)} />
        <Metric label={nl ? 'Herstel na grootste daling' : 'Recovery after largest loss'} value={data.trough_date ? (data.recovery_date ? day(data.recovery_date) : (nl ? 'Niet waargenomen' : 'Not observed')) : '—'} />
      </div>
      <section className="overflow-x-auto rounded-lg border border-neutral-800/40 p-3">
        <h3 className="mb-2 text-sm font-semibold text-fg-strong">{nl ? 'Werkelijk waargenomen verloop' : 'Actual observed path'}</h3>
        <table className="w-full text-xs"><thead><tr className="text-fg-muted">
          <th className="py-2 text-left">{nl ? 'Datum' : 'Date'}</th><th className="text-right">{nl ? 'AIRS cumulatief · EUR' : 'AIRS cumulative · EUR'}</th><th className="text-right">{nl ? 'Daling vanaf piek' : 'Drawdown from peak'}</th>
        </tr></thead><tbody>{points.map((point) => <tr key={point.date} className="border-t border-neutral-800/30">
          <td className="py-2 text-fg-muted">{day(point.date)}{point.reference ? (nl ? ' · jaarreferentie' : ' · year reference') : ''}</td>
          <td className="text-right font-mono text-fg">{percent(point.cumulative_pct)}</td><td className="text-right font-mono text-fg">{percent(point.drawdown_pct)}</td>
        </tr>)}</tbody></table>
      </section>
      <ExposureTable title={nl ? 'Volledige maanden · werkelijk AIRS-rendement' : 'Complete months · actual AIRS returns'} rows={months.map((month) => ({ name: month.month, weight: month.return_pct, routes: [] }))} />
      <p className="text-xs text-fg-faint">{nl ? 'Maandrendementen afgeleid van opeenvolgende AIRS-cumulatieve maandultimo’s. Vermogensmutaties en stortingen worden niet als rendement gebruikt.' : 'Monthly returns derived from consecutive AIRS cumulative month-end observations. Changes in account value and deposits are not used as returns.'}</p>
    </>}
  </div>;
}

export default function AirsRiskPanel(props: Props) {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [view, setView] = useState<View>('overview');
  const exposure = airsRiskExposures(props.airsHoldings, props.bands);
  const labels: Record<View, string> = nl
    ? { overview: 'Overzicht', exposures: 'Blootstellingen', losses: 'Werkelijke verliezen', simulation: 'Simulatie huidige portefeuille' }
    : { overview: 'Overview', exposures: 'Exposures', losses: 'Actual losses', simulation: 'Current portfolio simulation' };
  return <div className="flex h-full min-h-0 flex-col gap-3">
    <header className="shrink-0">
      <h2 className="text-base font-semibold text-fg-strong">{nl ? 'Portefeuillerisico' : 'Portfolio risk'} · {props.portfolioName}</h2>
      <p className="mt-1 text-xs text-fg-faint">{nl ? 'AIRS-posities per' : 'AIRS holdings as of'} {day(props.portfolioAsOf)} · {nl ? 'Laatst opgehaald' : 'Fetched'} {props.portfolioFetchedAt ? new Date(props.portfolioFetchedAt).toLocaleString(nl ? 'nl-NL' : 'en-GB') : '—'}</p>
      <nav aria-label={nl ? 'Risicoweergaven' : 'Risk views'} className="mt-3 flex flex-wrap gap-2">
        {(Object.keys(labels) as View[]).map((key) => <button key={key} type="button" aria-pressed={key === view} onClick={() => setView(key)}
          className={`cursor-pointer rounded-lg border px-3 py-2 text-xs ${key === view ? 'border-accent-500 bg-accent-600 text-white' : 'border-neutral-800/40 text-fg-muted hover:bg-overlay/5'}`}>{labels[key]}</button>)}
      </nav>
    </header>
    <div className="min-h-0 flex-1 overflow-auto">
      {view === 'simulation' ? <div className="flex h-full min-h-[28rem] flex-col gap-3">
        <p className="shrink-0 rounded-lg border border-warn-500/20 p-3 text-xs text-fg-muted">{nl ? 'Huidige posities en gewichten toegepast op historische marktkoersen. Geen werkelijk AIRS-rekeningverloop, geen voorspelling. Kas, alternatieven en niet-geprijsde posities kunnen buiten de simulatie vallen; controleer de dekking per weergave.' : 'Current positions and weights applied to historical market prices. Not actual AIRS account history and not a forecast. Cash, alternatives and unpriced positions may be excluded; check each view’s coverage.'}</p>
        <div className="min-h-0 flex-1"><CurrentPortfolioSimulation {...props} /></div>
      </div> : <div className="space-y-4 pr-1">
        {view !== 'exposures' && <AccountHistory key={props.portefeuille} account={props.portefeuille} view={view} />}
        {view !== 'losses' && <>
          <div className="grid gap-3 sm:grid-cols-3">
            <Metric label={nl ? 'Grootste bedrijfspositie' : 'Largest company exposure'} value={percent(exposure.companies[0]?.weight)} detail={exposure.companies[0]?.name} />
            <Metric label={nl ? 'Top 5 bedrijven · hele portefeuille' : 'Top 5 companies · whole portfolio'} value={exposure.companies.length ? percent(exposure.topFive) : '—'} />
            <Metric label={nl ? 'Grootste certificaatroute' : 'Largest certificate route'} value={percent(exposure.wrappers[0]?.weight)} detail={exposure.wrappers[0]?.name} />
          </div>
          <section className="rounded-lg border border-neutral-800/40 p-3 text-xs text-fg-muted">
            <h3 className="mb-2 font-semibold text-fg-strong">{nl ? 'Beleidsgrenzen en datadekking' : 'Policy limits and data coverage'}</h3>
            {!exposure.complete ? <p>{nl ? 'Gewichten ontbreken of tellen niet op tot 100%; beleidsconformiteit niet beoordeeld.' : 'Weights are missing or do not total 100%; policy compliance has not been assessed.'}</p>
              : !(props.bands?.length) || props.bands.every((band) => band.min_pct == null && band.max_pct == null) ? <p>{nl ? 'Geen vastgelegde allocatiegrenzen; geen oordeel over beleidsconformiteit.' : 'No recorded allocation limits; policy compliance cannot be assessed.'}</p>
              : exposure.breaches.length ? <ul className="list-disc pl-4">{exposure.breaches.map((breach) => <li key={breach.name}>{breach.name}: {percent(breach.actual)} · {breach.direction === 'above' ? (nl ? 'boven maximum' : 'above maximum') : (nl ? 'onder minimum' : 'below minimum')} {percent(breach.limit)}</li>)}</ul>
                : <p>{nl ? 'Geen overschrijdingen van de vastgelegde allocatiegrenzen. Dit is geen algemene verklaring dat het risico laag is.' : 'No breaches of recorded allocation limits. This does not mean overall risk is low.'}</p>}
            <p className="mt-2">{nl ? 'Bedrijfsdekking' : 'Company coverage'}: {percent(exposure.companyCoverage)} · {nl ? 'Totaal aangeleverde gewichten' : 'Total supplied weights'}: {percent(exposure.total)}.</p>
            <p className="mt-2">{nl ? 'AIRS-waarderingen met opgeslagen model-look-through. Classificaties zijn aanvullende referentiedata. Onbekende of niet-doorgekeken fondsen blijven zichtbaar; geen opschaling naar 100%. Kredietkwaliteit, duration, liquiditeit en juridische uitgevende-instellingrisico’s zijn nog niet beoordeeld.' : 'AIRS valuations with stored model look-through. Classifications are supplementary reference data. Unknown/opaque funds remain uncovered; no rebasing to 100%. Credit quality, duration, liquidity and legal issuer risk have not yet been assessed.'}</p>
          </section>
          <div className="grid gap-4 lg:grid-cols-2">
            <ExposureTable title={nl ? 'Bedrijven · % hele portefeuille' : 'Companies · % of whole portfolio'} rows={view === 'overview' ? exposure.companies.slice(0, 5) : exposure.companies} />
            <ExposureTable title={nl ? 'Activaklassen' : 'Asset classes'} rows={exposure.classes} />
            {view === 'exposures' && <>
              <ExposureTable title={nl ? 'Certificaatroutes' : 'Certificate routes'} rows={exposure.wrappers} note={nl ? 'Routes zijn geen geverifieerde juridische issuer-exposures.' : 'Routes are not verified legal-issuer exposures.'} />
              <ExposureTable title={nl ? 'Sectoren · individuele aandelen' : 'Sectors · individual equities'} rows={exposure.sectors} />
              <ExposureTable title={nl ? 'Dubbele bedrijven via certificaten' : 'Companies repeated across certificates'} rows={exposure.repeated} />
              <ExposureTable title={nl ? 'Noteringsvaluta' : 'Quotation currencies'} rows={exposure.currencies} note={nl ? 'Geen economische valutablootstelling of bevestiging van afdekking.' : 'Not economic currency exposure or confirmation of hedging.'} />
            </>}
          </div>
        </>}
      </div>}
    </div>
  </div>;
}
