'use client';

import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { useLang } from '../../../lib/i18n';
import { startJob } from '../../../lib/stores/jobs';
import type { MonthlyPosition, PerformanceBundle } from './monthlyReport';
import InfoTip from '../InfoTip';
import { profileBenchmarkLabel } from './analyseCopy';
import ViaChips from './ViaChips';
import { invalidatePerformanceBundle, loadPerformanceBundle, readPerformanceBundle } from './monthlyReportPrefetch';

const box = 'rounded-xl border border-neutral-800/40 bg-elevated';
const ctl = 'rounded-lg border border-neutral-700 bg-elevated px-3 py-2 text-xs text-fg-muted hover:border-accent-500/50 disabled:opacity-50';
const tone = (value?: number | null) => value == null ? 'text-fg-subtle' : value < 0 ? 'text-neg-400' : value > 0 ? 'text-pos-400' : 'text-fg-strong';

export default function MonthlyReportPanel({ portefeuille, name, variant }: { portefeuille?: string | null; name: string; variant?: string | null }) {
  const [lang] = useLang();
  const benchmarkLabel = profileBenchmarkLabel(lang, variant);
  const nl = lang === 'nl', locale = nl ? 'nl-NL' : 'en-GB';
  const text = (en: string, dutch: string) => nl ? dutch : en;
  const [bundle, setBundle] = useState<PerformanceBundle | null>(() => portefeuille ? readPerformanceBundle(portefeuille) : null);
  const [yearSelection, setYearSelection] = useState<string | null>(null);
  const [monthSelection, setMonthSelection] = useState<string | null>(null);
  const [lookThrough, setLookThrough] = useState(false);
  const [pending, setPending] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [jobError, setJobError] = useState<string | null>(null);
  const [reload, setReload] = useState(0);
  const [detailsHeight, setDetailsHeight] = useState(0);
  const detailsElement = useRef<HTMLDivElement>(null);
  const mounted = useRef(true);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);

  useEffect(() => {
    if (!portefeuille) return;
    let active = true;
    setError(null);
    setPending(true);
    void loadPerformanceBundle(portefeuille)
      .then((data) => { if (active) setBundle(data); })
      .catch((err) => { if (active) setError(String(err.message ?? err)); })
      .finally(() => { if (active) setPending(false); });
    return () => { active = false; };
  }, [portefeuille, reload]);

  const data = bundle?.portefeuille === portefeuille ? bundle : null;
  const today = data?.current_month ?? '';
  const year = yearSelection && data?.years[yearSelection] ? yearSelection : today.slice(0, 4);
  const report = data?.years[year];
  const month = monthSelection ?? '';
  const detailsOpen = !!report?.details[month];
  const selectedReport = report?.details[month];
  const perf = report?.rows[month];
  const expanded = selectedReport?.snapshot?.lookthrough_positions;
  const showConstituents = lookThrough && expanded !== undefined;
  const positions = showConstituents ? expanded : selectedReport?.snapshot?.positions;
  const reconciliation = selectedReport?.snapshot?.reconciliation;
  const unavailableCertificates = selectedReport?.snapshot?.lookthrough_unavailable ?? [];
  const openingWeightDate = selectedReport?.snapshot?.opening_weight_date;

  const openMonth = (key: string) => {
    setMonthSelection(key === month ? null : key);
    setJobError(null);
  };

  const refreshMonth = async (kind: 'monthly-report' | 'monthly-benchmark') => {
    if (!portefeuille || !month || busy) return;
    setBusy(true);
    setJobError(null);
    try {
      const { done } = await startJob(`${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/${kind}/job?month=${month}`, name);
      const job = await done;
      if (job.status !== 'done') throw new Error(job.summary ?? text('Refresh incomplete.', 'Verversing onvolledig.'));
    } catch (err) {
      if (mounted.current) setJobError(err instanceof Error ? err.message : String(err));
    } finally {
      invalidatePerformanceBundle(portefeuille);
      if (mounted.current) { setBusy(false); setReload((n) => n + 1); }
    }
  };
  useLayoutEffect(() => {
    const element = detailsElement.current;
    if (!detailsOpen || !element || showConstituents) return;
    const measure = () => {
      const height = Math.ceil(element.getBoundingClientRect().height);
      // Keep the largest loaded detail footprint, including while loading or
      // collapsed, so the modal's scroll range cannot shrink beneath the user.
      setDetailsHeight((previous) => Math.max(previous, height));
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, [detailsOpen, selectedReport, showConstituents]);
  const pct = (value?: number | null) => value == null || !Number.isFinite(value) ? '—'
    : `${new Intl.NumberFormat(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2, signDisplay: 'exceptZero' }).format(value)}%`;
  const pp = (value?: number | null) => pct(value).replace('%', 'pp');
  const weight = (value?: number | null) => value == null ? '—' : `${new Intl.NumberFormat(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(value)}%`;
  const monthLabel = (key: string, short = false) => new Date(`${key}-01T12:00:00`).toLocaleDateString(locale, { month: short ? 'short' : 'long', year: 'numeric' });
  const holdingDate = (date: string) => {
    const value = new Date(`${date}T12:00:00`);
    const weekday = value.toLocaleDateString(locale, { weekday: 'long' });
    const calendarDate = value.toLocaleDateString('nl-NL', { day: 'numeric', month: 'short', year: 'numeric' }).replaceAll('.', '').replace(' sep ', ' sept ');
    return `${weekday} ${calendarDate}`;
  };
  const eurPrice = (value: number) => new Intl.NumberFormat(locale, { style: 'currency', currency: 'EUR', minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(value);
  const shortDate = (date: string) => new Date(`${date}T12:00:00`).toLocaleDateString('nl-NL', { day: 'numeric', month: 'short', year: 'numeric' }).replaceAll('.', '').replace(' sep ', ' sept ');
  const contributionDetails = (position: MonthlyPosition) => {
    const calculation = position.contribution_calculation;
    if (!calculation) return <div className="space-y-2 text-xs"><p className="font-medium">{position.name}</p><p>{position.is_cash
      ? text('Cash balance movements from trades are not investment gains. AIRS income and accrued interest are shown separately below.', 'Kasmutaties door transacties zijn geen beleggingsresultaat. AIRS-inkomsten en opgelopen rente staan hieronder afzonderlijk.')
      : text('Contribution cannot yet be calculated reliably from the available AIRS valuations and transactions. A dash is not a zero contribution.', 'De bijdrage kan nog niet betrouwbaar worden berekend uit de beschikbare AIRS-waarderingen en transacties. Een streepje is geen nulbijdrage.')}</p></div>;
    return <div className="space-y-3 text-xs">
      <p className="font-medium text-fg-strong">{position.name} · {text('Contribution calculation', 'Berekening bijdrage')}</p>
      <table className="w-full"><tbody>{[
        [text('Closing value', 'Eindwaarde') + (calculation.closing_date ? ` · ${shortDate(calculation.closing_date)}` : ''), eurPrice(calculation.closing_value_eur)],
        [text('− Opening value', '− Beginwaarde') + (calculation.opening_date ? ` · ${shortDate(calculation.opening_date)}` : ''), eurPrice(calculation.opening_value_eur)],
        [text('+ Sale proceeds', '+ Verkoopopbrengsten'), eurPrice(calculation.sales_eur)],
        [text('− Purchases', '− Aankopen'), eurPrice(calculation.purchases_eur)],
        [text('= Position result · EUR', '= Positieresultaat · EUR'), eurPrice(calculation.result_eur)],
        [text('Portfolio opening capital', 'Beginvermogen portefeuille'), eurPrice(calculation.portfolio_opening_capital_eur)],
      ].map(([label, value]) => <tr key={label} className="border-t border-neutral-800/40"><td className="py-2 pr-4 text-left">{label}</td><td className="py-2 text-right font-mono tabular-nums">{value}</td></tr>)}</tbody></table>
      <p className="font-mono">{eurPrice(calculation.result_eur)} / {eurPrice(calculation.portfolio_opening_capital_eur)} × 100 = {pp(position.contribution_pp)}</p>
      {calculation.trades.length > 0 && <div><p className="mb-1 font-medium">{text('AIRS transactions', 'AIRS-transacties')}</p><ul className="max-h-40 space-y-1 overflow-auto">{calculation.trades.map((trade, i) => <li key={i} className="flex justify-between gap-4"><span>{shortDate(trade.date)} · {trade.kind === 'buy' ? text('Bought', 'Aangekocht') : text('Sold', 'Verkocht')}</span><span className="font-mono">{eurPrice(trade.amount_eur)}</span></li>)}</ul></div>}
      <p className="text-fg-subtle">{text('Source: AIRS valuations, transactions and opening portfolio capital. Income is shown separately. Calculations use full precision; EUR values here are rounded to two decimals.', 'Bron: AIRS-waarderingen, transacties en beginvermogen. Inkomsten staan afzonderlijk. De berekening gebruikt volledige precisie; EUR-bedragen zijn hier op twee decimalen afgerond.')}</p>
      {reconciliation?.status !== 'reconciled' && <p className="text-warn-300">{text('The month as a whole is not yet reconciled with AIRS.', 'De maand als geheel sluit nog niet aan op AIRS.')}</p>}
    </div>;
  };
  const benchmarkDetails = (key: string) => {
    const benchmark = report?.weighted_benchmark[key];
    return <div className="space-y-3 text-xs">
    <p className="font-medium text-fg-strong">{monthLabel(key)} · {benchmarkLabel}</p>
    <div className="overflow-x-auto"><table className="w-full text-left"><thead><tr>
      {[text('ETF', 'ETF'), text('Weight', 'Gewicht'), text('Opening · EUR', 'Begin · EUR'), text('Closing · EUR', 'Eind · EUR'), text('Return', 'Rendement')].map((label) => <th key={label} className="px-2 py-2 font-medium text-fg-subtle">{label}</th>)}
    </tr></thead><tbody>{benchmark?.components?.map((component) => <tr key={component.ticker} className="border-t border-neutral-800/40">
      <td className="px-2 py-2 font-medium">{component.ticker}</td>
      <td className="px-2 py-2 tabular-nums">{component.weight_pct}%</td>
      <td className="px-2 py-2"><span className="block tabular-nums">{component.start_price_eur != null ? eurPrice(component.start_price_eur) : '—'}</span><span className="text-fg-subtle">{holdingDate(component.start_date)}</span></td>
      <td className="px-2 py-2"><span className="block tabular-nums">{component.end_price_eur != null ? eurPrice(component.end_price_eur) : '—'}</span><span className="text-fg-subtle">{holdingDate(component.end_date)}</span></td>
      <td className={`px-2 py-2 tabular-nums ${tone(component.return_pct)}`}>{pct(component.return_pct)}</td>
    </tr>)}</tbody></table></div>
    <p>{text('Each ETF return = (closing price in EUR ÷ opening price in EUR − 1) × 100. The opening price is the last available close on or before the previous month-end; the closing price matches the AIRS observation date for this month.', 'Elk ETF-rendement = (eindkoers in EUR ÷ beginkoers in EUR − 1) × 100. De beginkoers is de laatst beschikbare slotkoers op of vóór het vorige maandeinde; de eindkoers sluit aan op de AIRS-waarnemingsdatum van deze maand.')}</p>
    <p className="text-fg-subtle">{text('Weighted return = sum of each ETF return × its policy weight ÷ 100. Prices per unit in EUR, including currency movements. Source: stored Yahoo Finance ETF prices.', 'Gewogen rendement = som van elk ETF-rendement × het beleidsgewicht ÷ 100. Prijzen per stuk in EUR, inclusief valuta-effecten. Bron: opgeslagen Yahoo Finance ETF-prijzen.')}</p>
    <p className="text-fg-subtle">{text('Foreign-currency prices are converted using stored exchange rates at each endpoint. Calculations use full precision; the displayed prices and returns are rounded.', 'Koersen in vreemde valuta worden omgerekend met opgeslagen wisselkoersen aan het begin en einde van de periode. De berekening gebruikt volledige precisie; getoonde koersen en rendementen zijn afgerond.')}</p>
    {!!benchmark?.components?.length && <p className="font-mono">{benchmark.components.map((component) => `(${pct(component.return_pct)} × ${component.weight_pct} / 100)`).join(' + ')} = {pct(benchmark.return_pct)}</p>}
    {!benchmark?.components?.length && <p className="text-fg-subtle">{text('The underlying price and weight details are not available for this month.', 'De onderliggende koers- en wegingsdetails zijn voor deze maand niet beschikbaar.')}</p>}
  </div>;
  };
  const summary = report?.summary;
  const historical = year < today.slice(0, 4);
  const benchmarkForMonth = (key: string) => report?.weighted_benchmark[key]?.return_pct;
  const unavailable = (reason?: string) => reason === 'unclassified_activity' || reason === 'quantity_not_reconciled'
    ? text('Position movement needs review', 'Positiemutatie controleren')
    : reason === 'ambiguous_position'
      ? text('Security value unclear in AIRS', 'Effectenwaarde onduidelijk in AIRS')
      : reason === 'identity_changed'
      ? text('Position identity needs review', 'Positie-identiteit controleren')
      : text('AIRS period data incomplete', 'AIRS-periodegegevens onvolledig');

  return <section className="space-y-5 pb-6">
    <header className="flex flex-wrap items-start justify-between gap-4"><div>
      <p className="mb-1 text-[10px] uppercase tracking-[0.2em] text-accent-400">AIRS · Performance</p>
      <h2 className="text-xl font-semibold text-fg-strong">{name}</h2>
      <p className="mt-1 text-xs text-fg-subtle">{text('Choose a month to see its return and individual positions.', 'Kies een maand voor het rendement en de individuele posities.')}</p>
    </div><label className="flex items-center gap-2 text-xs text-fg-muted">{text('Year', 'Jaar')}
      <select aria-label={text('Reporting year', 'Rapportagejaar')} value={year} className={ctl}
        onChange={(event) => {
          setYearSelection(event.target.value);
          setMonthSelection(null);
          setJobError(null);
        }}>{data?.available_years.map((option) => <option key={option} value={option}>{option}</option>)}</select>
    </label></header>
    {!portefeuille && <p className="text-sm text-warn-400">{text('Link an AIRS account to view monthly positions.', 'Koppel een AIRS-rekening om de maandposities te bekijken.')}</p>}
    {error && <p role="alert" className="rounded-lg bg-neg-500/10 p-3 text-xs text-neg-300">{error}</p>}
    {jobError && <p role="alert" className="rounded-lg bg-neg-500/10 p-3 text-xs text-neg-300">{jobError}</p>}
    {pending && !report && <p className="text-sm text-fg-subtle">{text('Loading…', 'Laden…')}</p>}
    {historical && report && !summary?.complete && <p className="text-xs text-warn-300">{text('Year-end report incomplete. Figures below run through the dates shown; finalisation will retry during the daily refresh.', 'Jaareindrapport onvolledig. De cijfers lopen tot de getoonde datums; afronding wordt bij de dagelijkse verversing opnieuw geprobeerd.')}</p>}
    {report && <>
      <div className="grid grid-cols-1 items-stretch gap-3 sm:grid-cols-3" aria-label={text('Compounded returns', 'Samengestelde rendementen')}>
        {[
          { label: historical ? text('Annual return · EUR', 'Jaarrendement · EUR') : `${name} (YTD) €`, value: summary?.return_pct },
          { label: historical ? text('Annual weighted benchmark return · EUR', 'Gewogen benchmarkjaarrendement · EUR') : `${benchmarkLabel} (YTD) €`, value: summary?.benchmark_pct },
        ].map(({ label, value }) => <div key={label} className={`${box} flex items-center justify-between gap-4 px-5 py-4`}>
          <div><p className="text-sm text-fg-muted">{label}</p><p className="mt-1 text-xs text-fg-subtle">{summary?.start && summary.end ? `${holdingDate(summary.start)} ${text('to', 't/m')} ${holdingDate(summary.end)}` : year}</p>
            {value == null && <p className="mt-1 text-xs text-fg-faint">{text('Monthly data incomplete', 'Maandgegevens onvolledig')}</p>}</div>
          <span className={`font-mono text-2xl font-semibold tabular-nums ${tone(value)}`}>{pct(value)}</span>
        </div>)}
        <div className={`${box} flex items-center justify-between gap-4 px-5 py-4`}>
          <span className="text-sm text-fg-muted">{text('Excess', 'Excess')}</span>
          <span className={`font-mono text-2xl font-semibold tabular-nums ${tone(summary?.excess_pp)}`}>{summary?.excess_pp == null ? '—' : `${new Intl.NumberFormat(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2, signDisplay: 'exceptZero' }).format(summary?.excess_pp)}pp`}</span>
        </div>
      </div>
      <nav className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-5" aria-label={text('Months', 'Maanden')}>
        {report.months.map((key) => { const row = report.rows[key]; const active = detailsOpen && key === month;
          return <button key={key} type="button" disabled={busy} onClick={() => openMonth(key)} aria-expanded={active} aria-pressed={active} aria-controls="monthly-report-details"
            className={`group rounded-xl border p-4 text-left transition-colors ${active ? 'border-accent-400 bg-accent-500/10 ring-1 ring-accent-400/50' : 'border-neutral-800/40 bg-elevated hover:border-accent-500/50'}`}>
            <span className={`block capitalize text-xs ${active ? 'font-medium text-accent-300' : 'text-fg-subtle'}`}>{monthLabel(key, true)}</span>
            <span className={`mt-2 block font-mono text-xl tabular-nums ${tone(row?.rendement)}`}>{pct(row?.rendement)}</span>
            <span data-month-benchmark={key} className="mt-1 block text-xs text-fg-subtle">{benchmarkLabel} <span className={`font-mono tabular-nums ${tone(benchmarkForMonth(key))}`}>{pct(benchmarkForMonth(key))}</span><InfoTip className="ml-1" wide content={benchmarkDetails(key)} /></span>
            <span className={`mt-3 block text-[11px] ${active ? 'text-accent-300' : 'text-fg-faint group-hover:text-accent-300'}`}>{text('View positions', 'Bekijk posities')}</span>
          </button>; })}
      </nav>
      <div data-month-detail-space style={detailsHeight ? { minHeight: detailsHeight } : undefined}>
      {detailsOpen && <div ref={detailsElement} id="monthly-report-details" aria-busy={pending || busy} className="min-h-80 space-y-4 border-t border-neutral-800/40 pt-5">
        <div className="flex flex-wrap items-start justify-between gap-3"><div>
          <h3 className="text-lg font-medium capitalize text-fg-strong">{monthLabel(month)}</h3>
          <p className="mt-1 text-xs text-fg-subtle">{perf?.periode ? `${text('Through', 'T/m')} ${shortDate(perf.periode)}` : text('Return unavailable', 'Rendement ontbreekt')}{month === today ? ` · ${text('Month to date', 'Lopende maand')}` : ''}</p>
        </div><div className="flex gap-2">
          <button type="button" className={ctl} onClick={() => void refreshMonth('monthly-report')} disabled={busy || pending || !!report.warning}>{busy ? text('Reading AIRS…', 'AIRS ophalen…') : selectedReport?.snapshot ? text('Refresh month', 'Maand verversen') : text('Load month from AIRS', 'Maand ophalen uit AIRS')}</button>
        </div></div>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <div className={`${box} flex items-center justify-between gap-4 px-5 py-4`}><span className="text-sm text-fg-muted">{name}</span>
            <span className={`font-mono text-2xl font-semibold tabular-nums ${tone(perf?.rendement)}`}>{pct(perf?.rendement)}</span></div>
          <div className={`${box} flex items-center justify-between gap-4 px-5 py-4`}><span className="text-sm text-fg-muted">{benchmarkLabel}<InfoTip className="ml-2" wide content={benchmarkDetails(month)} /></span>
            <span className={`font-mono text-2xl font-semibold tabular-nums ${tone(benchmarkForMonth(month))}`}>{pct(benchmarkForMonth(month))}</span></div>
        </div>
        {report.warning && <p className="text-xs text-warn-300">{report.warning}</p>}
        {selectedReport?.benchmark_needs_refresh && <p className="text-xs text-warn-300">
          {text('Benchmark is not available yet.', 'Benchmark is nog niet beschikbaar.')} <button type="button" className="underline" disabled={busy || pending} onClick={() => void refreshMonth('monthly-benchmark')}>{text('Refresh benchmark', 'Benchmark verversen')}</button>
        </p>}
        {selectedReport?.needs_refresh && !report.warning && selectedReport.refresh_reason
          && selectedReport.refresh_reason !== 'incomplete_report' && <p className="text-xs text-warn-300">
          {selectedReport.refresh_reason === 'ambiguous_position'
            ? text('For the marked position, AIRS lists several amounts under the same name. The saved valuation does not show which amount is the security value and which may be accrued interest. Without a verified security value at both dates, the opening weight, return and contribution are left blank.', 'Bij de gemarkeerde positie staan meerdere bedragen onder dezelfde naam in AIRS. Uit de opgeslagen waardering blijkt niet welk bedrag de effectenwaarde is en welk bedrag mogelijk opgelopen rente is. Zonder bevestigde effectenwaarde op beide datums blijven begingewicht, rendement en bijdrage leeg.')
            : selectedReport.refresh_reason === 'outdated_report'
              ? text('The saved position report does not cover the latest valuation date for this month.', 'Het opgeslagen positierapport loopt niet tot de laatste waarderingsdatum van deze maand.')
              : selectedReport.refresh_reason === 'preparation_failed'
                ? text('AIRS could not provide all position data for this month.', 'AIRS kon niet alle positiegegevens voor deze maand leveren.')
                : text('No position report has been saved for this month yet.', 'Voor deze maand is nog geen positierapport opgeslagen.')}
        </p>}
        <div className={`${box} overflow-hidden`}>
          <div className="flex flex-wrap items-center justify-between gap-2 px-5 pt-4">
            <h4 className="text-sm font-medium text-fg-strong">{text('Individual positions', 'Individuele posities')}</h4>
            {expanded && <label className="flex items-center gap-2 text-xs text-fg-muted">
              <input type="checkbox" checked={lookThrough} onChange={(event) => setLookThrough(event.target.checked)} />
              {text('Look through certificates', 'Door certificaten heen kijken')}
            </label>}
          </div>
          {showConstituents && unavailableCertificates.length > 0 && <p className="px-5 pt-2 text-xs text-fg-subtle">
            {text('Dated holdings unavailable for', 'Geen gedateerde posities beschikbaar voor')} {unavailableCertificates.join(', ')}.
          </p>}
          {!positions ? <p className="p-5 text-sm text-fg-subtle">{busy ? text('Reading AIRS position reports…', 'AIRS-positierapporten ophalen…') : text('Position results are not available yet.', 'De positieresultaten zijn nog niet beschikbaar.')}</p>
            : !positions.length ? <p className="p-5 text-sm text-fg-subtle">{text('No positions were found in the available AIRS reports.', 'Geen posities gevonden in de beschikbare AIRS-rapporten.')}</p>
            : <div className="overflow-auto p-3"><table className="w-full text-sm"><thead><tr>
              <th className="px-3 py-3 text-left text-xs font-medium text-fg-subtle">{text('Position', 'Positie')}</th>
              {showConstituents && <th className="px-3 py-3 text-left text-xs font-medium text-fg-subtle">Via</th>}
              {[text('Opening weight (%)', 'Begingewicht (%)'), text('Return in EUR (%)', 'Rendement in EUR (%)'), text('Contribution (pp)', 'Bijdrage (pp)'), text('Holding period', 'Periode in portefeuille')].map((label, i) => <th key={label} className={`px-3 py-3 text-xs font-medium text-fg-subtle ${i < 3 ? 'text-right' : 'text-left'}`}>{label}</th>)}
            </tr></thead><tbody>{positions.map((position, i) => <tr key={`${position.isin || position.name}-${i}`} className="border-t border-neutral-800/40">
              <td className="px-3 py-3 font-medium text-fg-muted">{position.name}{position.status === 'bought' || position.status === 'sold' ? <span className="ml-2 text-[10px] text-fg-faint">{position.status === 'bought' ? text('Bought', 'Aangekocht') : text('Sold', 'Verkocht')}</span> : null}</td>
              {showConstituents && <td className="px-3 py-3"><ViaChips names={position.certificate ? [position.certificate] : []}
                weights={position.via} weightTitle={position.certificate
                  ? text(`This row: ${position.certificate}. Percentages are opening weights within each selection.`, `Deze regel: ${position.certificate}. Percentages zijn begingewichten binnen elke selectie.`)
                  : undefined} /></td>}
              <td className="px-3 py-3 text-right font-mono tabular-nums text-fg-muted">{weight(position.opening_weight_pct)}{position.opening_weight_pct != null && openingWeightDate && <InfoTip className="ml-2" text={`${text('Opening weight · AIRS valuation', 'Begingewicht · AIRS-waardering')}\n${shortDate(openingWeightDate)}`} />}</td>
              <td className={`px-3 py-3 text-right font-mono tabular-nums ${tone(position.return_pct)}`}>{pct(position.return_pct)}{position.start_price_eur != null && position.end_price_eur != null && <InfoTip className="ml-2" content={<div className="space-y-2 text-xs">
                <p className="font-medium text-fg-strong">{text('AIRS price per unit in EUR', 'AIRS-prijs per stuk in EUR')}</p>
                <table className="w-full"><tbody>
                  <tr><td className="py-1 pr-3">{position.start_date ? holdingDate(position.start_date) : '—'}</td><td className="py-1 text-right font-mono">{eurPrice(position.start_price_eur)}</td></tr>
                  <tr><td className="py-1 pr-3">{position.end_date ? holdingDate(position.end_date) : '—'}</td><td className="py-1 text-right font-mono">{eurPrice(position.end_price_eur)}</td></tr>
                </tbody></table>
                <p className="text-fg-subtle">{text('Includes currency movements; excludes income.', 'Inclusief valuta-effecten; exclusief inkomsten.')}</p>
              </div>} />}</td>
              <td className={`px-3 py-3 text-right font-mono tabular-nums ${tone(position.contribution_pp)}`}>{pp(position.contribution_pp)}{position.certificate
                ? <InfoTip className="ml-2" text={text('AIRS reports the contribution at certificate level.', 'AIRS rapporteert de bijdrage op certificaatniveau.')} />
                : <InfoTip className="ml-2" wide content={contributionDetails(position)} />}</td>
              <td className="px-3 py-3 text-xs text-fg-subtle">{!position.is_cash && (position.start_date && position.end_date ? `${holdingDate(position.start_date)} to ${holdingDate(position.end_date)}` : unavailable(position.reason))}
                {!!position.ambiguous_valuation_dates?.length && <span className="block">{position.ambiguous_valuation_dates.map(shortDate).join(', ')}</span>}
              </td>
            </tr>)}{reconciliation?.adjustments.map((adjustment) => <tr key={adjustment.kind} className="border-t border-neutral-800/40 text-fg-muted">
              <td className="px-3 py-3">{adjustment.kind === 'opbrengsten' ? text('Income · AIRS (not allocated to positions)', 'Inkomsten · AIRS (niet aan posities toegerekend)') : text('Accrued interest change · AIRS', 'Mutatie opgelopen rente · AIRS')}</td>
              {showConstituents && <td />}
              <td className="px-3 py-3 text-right">—</td><td className="px-3 py-3 text-right">—</td><td className={`px-3 py-3 text-right font-mono ${tone(adjustment.contribution_pp)}`}>{pp(adjustment.contribution_pp)}</td><td />
            </tr>)}</tbody>{reconciliation && <tfoot className="border-t border-neutral-700 text-fg-strong">
              {[
                [text('Known contributions', 'Bekende bijdragen'), pp(reconciliation.contribution_sum_pp)],
                [name, pct(reconciliation.monthly_return_pct)],
                [text('Unexplained difference', 'Onverklaard verschil'), pp(reconciliation.difference_pp)],
              ].map(([label, value]) => <tr key={label}><th colSpan={showConstituents ? 4 : 3} className="px-3 py-2 text-left font-medium">{label}</th><td className="px-3 py-2 text-right font-mono">{value}</td><td /></tr>)}
              {showConstituents && <tr><td colSpan={6} className="px-3 pb-2 text-xs text-fg-subtle">
                {text('AIRS totals use the portfolio’s direct holdings and certificate-level contributions.', 'De AIRS-totalen gebruiken directe posities en bijdragen op certificaatniveau.')}
              </td></tr>}
            </tfoot>}</table></div>}
        </div>
      </div>}
      </div>
    </>}
  </section>;
}
