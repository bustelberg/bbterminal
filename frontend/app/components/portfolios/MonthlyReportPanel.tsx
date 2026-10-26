'use client';

import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { useLang } from '../../../lib/i18n';
import { startJob } from '../../../lib/stores/jobs';
import type { MonthlyPosition, MonthlyReport } from './monthlyReport';
import { savedContributionCalculation } from './monthlyReport';
import InfoTip from '../InfoTip';

const box = 'rounded-xl border border-neutral-800/40 bg-elevated';
const ctl = 'rounded-lg border border-neutral-700 bg-elevated px-3 py-2 text-xs text-fg-muted hover:border-accent-500/50 disabled:opacity-50';
const tone = (value?: number | null) => value == null ? 'text-fg-subtle' : value < 0 ? 'text-neg-400' : value > 0 ? 'text-pos-400' : 'text-fg-strong';

type OverviewReturns = { portfolio: number | null | undefined; benchmark: number | null | undefined; asOf?: string | null; from?: string | null };

export default function MonthlyReportPanel({ portefeuille, name, overviewReturns }: { portefeuille?: string | null; name: string; overviewReturns?: OverviewReturns }) {
  const [lang] = useLang();
  const nl = lang === 'nl', locale = nl ? 'nl-NL' : 'en-GB';
  const text = (en: string, dutch: string) => nl ? dutch : en;
  const today = new Intl.DateTimeFormat('sv-SE', { timeZone: 'Europe/Amsterdam', year: 'numeric', month: '2-digit' }).format(new Date());
  const [month, setMonth] = useState(today);
  const [detailsOpen, setDetailsOpen] = useState(false);
  const [report, setReport] = useState<MonthlyReport | null>(null);
  const [pending, setPending] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [jobError, setJobError] = useState<string | null>(null);
  const [reload, setReload] = useState(0);
  const [selection, setSelection] = useState(0);
  const [detailsHeight, setDetailsHeight] = useState(0);
  const detailsElement = useRef<HTMLDivElement>(null);
  const saved = useRef(new Map<string, MonthlyReport>());
  const automaticAttempts = useRef(new Set<string>());
  const fetching = useRef(false);
  const mounted = useRef(true);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  useEffect(() => {
    let active = true;
    setError(null); setPending(true);
    if (!portefeuille) { setReport(null); setPending(false); return; }
    setReport((previous) => previous?.portefeuille === portefeuille ? previous : null);
    const key = `${portefeuille}|${month}`;
    const cached = saved.current.get(key);
    // Recheck the current month against the database's valuation cutoff. A
    // completed historical month can be reopened from this view's memory cache.
    if (month !== today && cached && !cached.needs_refresh && !cached.warning && cached.snapshot?.positions
      && cached.snapshot.positions.every((position) => Object.hasOwn(position, 'contribution_calculation'))) {
      setReport(cached); setPending(false); return;
    }
    void apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/monthly-report?month=${month}`,
      { noReadCache: true, cache: 'no-store' }).then(async (res) => {
      if (!res.ok) { const body = await res.json().catch(() => ({})); throw new Error(body.detail ?? `HTTP ${res.status}`); }
      return res.json() as Promise<MonthlyReport>;
    }).then((data) => { if (active) { saved.current.set(key, data); setReport(data); } })
      .catch((err) => { if (active) setError(String(err.message ?? err)); })
      .finally(() => { if (active) setPending(false); });
    return () => { active = false; };
  }, [portefeuille, month, reload, today]);

  const openMonth = (key: string) => {
    if (key === month && detailsOpen) { setDetailsOpen(false); return; }
    automaticAttempts.current.delete(`${portefeuille}|${key}`);
    setSelection((n) => n + 1);
    setMonth(key); setDetailsOpen(true); setJobError(null);
  };
  const fetchMonth = useCallback(async () => {
    if (!portefeuille || fetching.current) return;
    fetching.current = true;
    automaticAttempts.current.add(`${portefeuille}|${month}`);
    saved.current.delete(`${portefeuille}|${month}`);
    setBusy(true); setJobError(null);
    try {
      const { done } = await startJob(`${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/monthly-report/job?month=${month}`, `${name} · Performance ${month}`);
      const job = await done;
      if (mounted.current && job.status === 'failed') setJobError(job.summary ?? (nl ? 'Het rapport kon niet worden voltooid.' : 'Report could not be completed.'));
    } catch (err) {
      if (mounted.current) setJobError(err instanceof Error ? err.message : String(err));
    } finally {
      fetching.current = false;
      if (mounted.current) { setBusy(false); setReload((n) => n + 1); }
    }
  }, [portefeuille, month, name, nl]);
  useEffect(() => {
    if (!detailsOpen || pending || busy || !report || report.warning
      || report.month !== month || report.portefeuille !== portefeuille) return;
    if (!report.needs_refresh && report.snapshot?.positions) return;
    const key = `${portefeuille}|${month}`;
    // One automatic attempt per selected month. A partial/failed download must
    // not trigger a retry loop when its saved result is read back afterwards.
    if (automaticAttempts.current.has(key)) return;
    automaticAttempts.current.add(key);
    void fetchMonth();
  }, [detailsOpen, pending, busy, report, month, portefeuille, fetchMonth, selection]);
  const selectedReport = report?.month === month && report.portefeuille === portefeuille ? report : null;
  const performance = selectedReport?.snapshot?.sections.performance;
  const perf = performance?.status === 'ok' ? performance.data : report?.months.find((r) => r.month === month);
  const positions = selectedReport?.snapshot?.positions;
  const reconciliation = selectedReport?.snapshot?.reconciliation;
  const openingValuation = selectedReport?.snapshot?.sections.opening;
  const openingWeightDate = openingValuation?.status === 'ok' ? openingValuation.data?.as_of : null;
  useLayoutEffect(() => {
    const element = detailsElement.current;
    if (!detailsOpen || !element) return;
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
  }, [detailsOpen, selectedReport]);
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
    const calculation = savedContributionCalculation(selectedReport, position);
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
      <p className="font-mono">{eurPrice(calculation.result_eur)} / {eurPrice(calculation.portfolio_opening_capital_eur)} × 100 = {pp(100 * calculation.result_eur / calculation.portfolio_opening_capital_eur)}</p>
      {calculation.trades.length > 0 && <div><p className="mb-1 font-medium">{text('AIRS transactions', 'AIRS-transacties')}</p><ul className="max-h-40 space-y-1 overflow-auto">{calculation.trades.map((trade, i) => <li key={i} className="flex justify-between gap-4"><span>{shortDate(trade.date)} · {trade.kind === 'buy' ? text('Bought', 'Aangekocht') : text('Sold', 'Verkocht')}</span><span className="font-mono">{eurPrice(trade.amount_eur)}</span></li>)}</ul></div>}
      <p className="text-fg-subtle">{text('Source: AIRS valuations, transactions and opening portfolio capital. Income is shown separately. Calculations use full precision; EUR values here are rounded to two decimals.', 'Bron: AIRS-waarderingen, transacties en beginvermogen. Inkomsten staan afzonderlijk. De berekening gebruikt volledige precisie; EUR-bedragen zijn hier op twee decimalen afgerond.')}</p>
      {reconciliation?.status !== 'reconciled' && <p className="text-warn-300">{text('The month as a whole is not yet reconciled with AIRS.', 'De maand als geheel sluit nog niet aan op AIRS.')}</p>}
    </div>;
  };
  const benchmark = selectedReport?.weighted_benchmark?.[month];
  const benchmarkDetails = <div className="space-y-3 text-xs">
    <p className="font-medium text-fg-strong">{text('Monthly weighted benchmark · EUR', 'Gewogen maandbenchmark · EUR')}</p>
    <div className="overflow-x-auto"><table className="w-full text-left"><thead><tr>
      {[text('ETF', 'ETF'), text('Weight', 'Gewicht'), text('Opening · EUR', 'Begin · EUR'), text('Closing · EUR', 'Eind · EUR'), text('Return', 'Rendement')].map((label) => <th key={label} className="px-2 py-2 font-medium text-fg-subtle">{label}</th>)}
    </tr></thead><tbody>{benchmark?.components?.map((component) => <tr key={component.ticker} className="border-t border-neutral-800/40">
      <td className="px-2 py-2 font-medium">{component.ticker}</td>
      <td className="px-2 py-2 tabular-nums">{component.weight_pct}%</td>
      <td className="px-2 py-2"><span className="block tabular-nums">{component.start_price_eur != null ? eurPrice(component.start_price_eur) : '—'}</span><span className="text-fg-subtle">{holdingDate(component.start_date)}</span></td>
      <td className="px-2 py-2"><span className="block tabular-nums">{component.end_price_eur != null ? eurPrice(component.end_price_eur) : '—'}</span><span className="text-fg-subtle">{holdingDate(component.end_date)}</span></td>
      <td className={`px-2 py-2 tabular-nums ${tone(component.return_pct)}`}>{pct(component.return_pct)}</td>
    </tr>)}</tbody></table></div>
    <p className="text-fg-subtle">{text('Weighted return = sum of each ETF return × its policy weight. Prices per unit in EUR, including currency movements. Source: stored Yahoo Finance ETF prices.', 'Gewogen rendement = som van elk ETF-rendement × het beleidsgewicht. Prijzen per stuk in EUR, inclusief valuta-effecten. Bron: opgeslagen Yahoo Finance ETF-prijzen.')}</p>
  </div>;
  const months = Array.from({ length: month.slice(0, 4) === today.slice(0, 4) ? Number(today.slice(5, 7)) : 12 }, (_, i) => `${month.slice(0, 4)}-${String(i + 1).padStart(2, '0')}`);
  const monthPerformance = (key: string) => key === month && perf ? perf : report?.months.find((row) => row.month === key);
  const compound = (values: (number | null | undefined)[]) => values.some((value) => value == null || !Number.isFinite(value))
    ? null : (values.reduce<number>((growth, value) => growth * (1 + value! / 100), 1) - 1) * 100;
  // The Analyse headline is AIRS's published cumulative return, not a chain of
  // monthly observations. Its policy benchmark is buy-and-hold over the full
  // window; chaining monthly policy returns would imply monthly rebalancing.
  const useOverview = !!overviewReturns && (!overviewReturns.asOf || overviewReturns.asOf.slice(0, 4) === month.slice(0, 4));
  const rounded = (value: number | null | undefined) => value == null ? null : Math.round(value * 100) / 100;
  const portfolioCompounded = useOverview ? rounded(overviewReturns.portfolio) : compound(months.map((key) => monthPerformance(key)?.rendement));
  const benchmarkCompounded = useOverview ? rounded(overviewReturns.benchmark) : compound(months.map((key) => report?.weighted_benchmark?.[key]?.return_pct));
  const compoundedExcess = portfolioCompounded != null && benchmarkCompounded != null ? portfolioCompounded - benchmarkCompounded : null;
  const compoundedThrough = useOverview ? overviewReturns.asOf : monthPerformance(months[months.length - 1])?.periode;
  const unavailable = (reason?: string) => reason === 'unclassified_activity' || reason === 'quantity_not_reconciled'
    ? text('Position movement needs review', 'Positiemutatie controleren')
    : reason === 'identity_changed' || reason === 'ambiguous_position'
      ? text('Position identity needs review', 'Positie-identiteit controleren')
      : text('AIRS period data incomplete', 'AIRS-periodegegevens onvolledig');

  return <section className="space-y-5 pb-6">
    <header className="flex flex-wrap items-start justify-between gap-4"><div>
      <p className="mb-1 text-[10px] uppercase tracking-[0.2em] text-accent-400">AIRS · Performance</p>
      <h2 className="text-xl font-semibold text-fg-strong">{name}</h2>
      <p className="mt-1 text-xs text-fg-subtle">{text('Choose a month to see its return and individual positions.', 'Kies een maand voor het rendement en de individuele posities.')}</p>
    </div></header>
    {!portefeuille && <p className="text-sm text-warn-400">{text('Link an AIRS account to view monthly positions.', 'Koppel een AIRS-rekening om de maandposities te bekijken.')}</p>}
    {error && <p role="alert" className="rounded-lg bg-neg-500/10 p-3 text-xs text-neg-300">{error}</p>}
    {jobError && <p role="alert" className="rounded-lg bg-neg-500/10 p-3 text-xs text-neg-300">{jobError}</p>}
    {pending && !report && <p className="text-sm text-fg-subtle">{text('Loading…', 'Laden…')}</p>}
    {report && <>
      <div className="grid grid-cols-1 items-stretch gap-3 sm:grid-cols-3" aria-label={text('Compounded returns', 'Samengestelde rendementen')}>
        {[
          { label: useOverview ? text('Return (YTD) · EUR', 'Rendement (YTD) · EUR') : text('Compounded portfolio return · EUR', 'Samengesteld portefeuillerendement · EUR'), value: portfolioCompounded },
          { label: useOverview ? text('Weighted benchmark return (YTD) · EUR', 'Gewogen benchmarkrendement (YTD) · EUR') : text('Compounded weighted benchmark · EUR', 'Samengestelde gewogen benchmark · EUR'), value: benchmarkCompounded },
        ].map(({ label, value }) => <div key={label} className={`${box} flex items-center justify-between gap-4 px-5 py-4`}>
          <div><p className="text-sm text-fg-muted">{label}</p><p className="mt-1 text-xs text-fg-subtle">{month.slice(0, 4)}{compoundedThrough ? ` · ${text('Through', 'T/m')} ${holdingDate(compoundedThrough)}` : ''}</p>
            {value == null && <p className="mt-1 text-xs text-fg-faint">{text('Monthly data incomplete', 'Maandgegevens onvolledig')}</p>}</div>
          <span className={`font-mono text-2xl font-semibold tabular-nums ${tone(value)}`}>{pct(value)}</span>
        </div>)}
        <div className={`${box} flex items-center justify-between gap-4 px-5 py-4`}>
          <span className="text-sm text-fg-muted">{text('Excess', 'Excess')}</span>
          <span className={`font-mono text-2xl font-semibold tabular-nums ${tone(compoundedExcess)}`}>{compoundedExcess == null ? '—' : `${new Intl.NumberFormat(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2, signDisplay: 'exceptZero' }).format(compoundedExcess)}pp`}</span>
        </div>
      </div>
      {useOverview && <p className="text-[11px] text-fg-faint">{text('Same YTD source and calculation as the Analyse overview: AIRS cumulative portfolio return and the full-period policy benchmark. Monthly figures below are separate period returns; compounding them can differ from these YTD figures.', 'Dezelfde YTD-bron en berekening als het Analyse-overzicht: cumulatief AIRS-portefeuillerendement en de beleidsbenchmark over de volledige periode. De maandcijfers hieronder zijn afzonderlijke perioderendementen; het samenstellen daarvan kan afwijken van deze YTD-cijfers.')}</p>}
      <nav className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-5" aria-label={text('Months', 'Maanden')}>
        {months.map((key) => { const row = monthPerformance(key); const active = detailsOpen && key === month;
          return <button key={key} type="button" disabled={busy} onClick={() => openMonth(key)} aria-expanded={active} aria-pressed={active} aria-controls="monthly-report-details"
            className={`group rounded-xl border p-4 text-left transition-colors ${active ? 'border-accent-400 bg-accent-500/10 ring-1 ring-accent-400/50' : 'border-neutral-800/40 bg-elevated hover:border-accent-500/50'}`}>
            <span className={`block capitalize text-xs ${active ? 'font-medium text-accent-300' : 'text-fg-subtle'}`}>{monthLabel(key, true)}</span>
            <span className={`mt-2 block font-mono text-xl tabular-nums ${tone(row?.rendement)}`}>{pct(row?.rendement)}</span>
            <span className="mt-1 block text-xs text-fg-subtle" title={report.weighted_benchmark?.[key]?.source}>{text('Weighted benchmark · EUR', 'Gewogen benchmark · EUR')} <span className={`font-mono tabular-nums ${tone(report.weighted_benchmark?.[key]?.return_pct)}`}>{pct(report.weighted_benchmark?.[key]?.return_pct)}</span></span>
            <span className={`mt-3 block text-[11px] ${active ? 'text-accent-300' : 'text-fg-faint group-hover:text-accent-300'}`}>{text('View positions', 'Bekijk posities')}</span>
          </button>; })}
      </nav>
      <div data-month-detail-space style={detailsHeight ? { minHeight: detailsHeight } : undefined}>
      {detailsOpen && <div ref={detailsElement} id="monthly-report-details" aria-busy={pending} className="min-h-80 space-y-4 border-t border-neutral-800/40 pt-5">
        {!selectedReport ? <p role="status" className="text-sm text-fg-subtle">{monthLabel(month)} · {error ? text('Report could not be loaded.', 'Rapport kon niet worden geladen.') : text('Loading…', 'Laden…')}</p> : <>
        <div className="flex flex-wrap items-start justify-between gap-3"><div>
          <h3 className="text-lg font-medium capitalize text-fg-strong">{monthLabel(month)}</h3>
          <p className="mt-1 text-xs text-fg-subtle">{perf?.periode ? `${text('Through', 'T/m')} ${perf.periode}` : text('Return unavailable', 'Rendement ontbreekt')}{month === today ? ` · ${text('Month to date', 'Lopende maand')}` : ''}</p>
        </div><div className="flex gap-2">
          <button type="button" className={ctl} onClick={() => void fetchMonth()} disabled={busy || pending || !!report.warning}>{busy ? text('Reading AIRS…', 'AIRS ophalen…') : report.snapshot ? text('Refresh month', 'Maand verversen') : text('Load month from AIRS', 'Maand ophalen uit AIRS')}</button>
        </div></div>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <div className={`${box} flex items-center justify-between gap-4 px-5 py-4`}><span className="text-sm text-fg-muted">{text('Monthly return · AIRS', 'Maandrendement · AIRS')}</span>
            <span className={`font-mono text-2xl font-semibold tabular-nums ${tone(perf?.rendement)}`}>{pct(perf?.rendement)}</span></div>
          <div className={`${box} flex items-center justify-between gap-4 px-5 py-4`}><span className="text-sm text-fg-muted">{text('Weighted benchmark return · EUR', 'Gewogen benchmarkrendement · EUR')}{benchmark && <InfoTip className="ml-2" wide content={benchmarkDetails} />}</span>
            <span className={`font-mono text-2xl font-semibold tabular-nums ${tone(selectedReport.weighted_benchmark?.[month]?.return_pct)}`}>{pct(selectedReport.weighted_benchmark?.[month]?.return_pct)}</span></div>
        </div>
        {report.warning && <p className="text-xs text-warn-300">{report.warning}</p>}
        {report.needs_refresh && !report.warning && <p className="text-xs text-warn-300">{busy ? text('Loading this month’s position results from AIRS…', 'De positieresultaten van deze maand worden uit AIRS opgehaald…') : text('Some position results are incomplete. Refresh this month to try again.', 'Sommige positieresultaten zijn onvolledig. Ververs deze maand om het opnieuw te proberen.')}</p>}
        <div className={`${box} overflow-hidden`}><h4 className="px-5 pt-4 text-sm font-medium text-fg-strong">{text('Individual positions', 'Individuele posities')}</h4>
          {!positions ? <p className="p-5 text-sm text-fg-subtle">{busy ? text('Reading AIRS position reports…', 'AIRS-positierapporten ophalen…') : text('Position results are not available yet.', 'De positieresultaten zijn nog niet beschikbaar.')}</p>
            : !positions.length ? <p className="p-5 text-sm text-fg-subtle">{text('No positions were found in the available AIRS reports.', 'Geen posities gevonden in de beschikbare AIRS-rapporten.')}</p>
            : <div className="overflow-auto p-3"><table className="w-full text-sm"><thead><tr>
              {[text('Position', 'Positie'), text('Opening weight (%)', 'Begingewicht (%)'), text('Return in EUR (%)', 'Rendement in EUR (%)'), text('Contribution (pp)', 'Bijdrage (pp)'), text('Holding period', 'Periode in portefeuille')].map((label, i) => <th key={label} className={`px-3 py-3 text-xs font-medium text-fg-subtle ${i > 0 && i < 4 ? 'text-right' : 'text-left'}`}>{label}</th>)}
            </tr></thead><tbody>{positions.map((position, i) => <tr key={`${position.isin || position.name}-${i}`} className="border-t border-neutral-800/40">
              <td className="px-3 py-3 font-medium text-fg-muted">{position.name}{position.status === 'bought' || position.status === 'sold' ? <span className="ml-2 text-[10px] text-fg-faint">{position.status === 'bought' ? text('Bought', 'Aangekocht') : text('Sold', 'Verkocht')}</span> : null}</td>
              <td className="px-3 py-3 text-right font-mono tabular-nums text-fg-muted">{weight(position.opening_weight_pct)}{position.opening_weight_pct != null && openingWeightDate && <InfoTip className="ml-2" text={`${text('Opening weight · AIRS valuation', 'Begingewicht · AIRS-waardering')}\n${shortDate(openingWeightDate)}`} />}</td>
              <td className={`px-3 py-3 text-right font-mono tabular-nums ${tone(position.return_pct)}`}>{pct(position.return_pct)}{position.start_price_eur != null && position.end_price_eur != null && <InfoTip className="ml-2" text={`${text('AIRS price per unit in EUR', 'AIRS-prijs per stuk in EUR')}\n${position.start_date ? holdingDate(position.start_date) : ''}: ${eurPrice(position.start_price_eur)}\n${position.end_date ? holdingDate(position.end_date) : ''}: ${eurPrice(position.end_price_eur)}\n${text('Includes currency movements; excludes income.', 'Inclusief valuta-effecten; exclusief inkomsten.')}`} />}</td>
              <td className={`px-3 py-3 text-right font-mono tabular-nums ${tone(position.contribution_pp)}`}>{pp(position.contribution_pp)}<InfoTip className="ml-2" wide content={contributionDetails(position)} /></td>
              <td className="px-3 py-3 text-xs text-fg-subtle">{position.is_cash ? text('Trading cash balance; income shown below', 'Handelskas; inkomsten hieronder') : position.start_date && position.end_date ? `${holdingDate(position.start_date)} to ${holdingDate(position.end_date)}` : unavailable(position.reason)}</td>
            </tr>)}{reconciliation?.adjustments.map((adjustment) => <tr key={adjustment.kind} className="border-t border-neutral-800/40 text-fg-muted">
              <td className="px-3 py-3">{adjustment.kind === 'opbrengsten' ? text('Income · AIRS (not allocated to positions)', 'Inkomsten · AIRS (niet aan posities toegerekend)') : text('Accrued interest change · AIRS', 'Mutatie opgelopen rente · AIRS')}</td>
              <td className="px-3 py-3 text-right">—</td><td className="px-3 py-3 text-right">—</td><td className={`px-3 py-3 text-right font-mono ${tone(adjustment.contribution_pp)}`}>{pp(adjustment.contribution_pp)}</td><td />
            </tr>)}</tbody>{reconciliation && <tfoot className="border-t border-neutral-700 text-fg-strong">
              {[
                [text('Known contributions', 'Bekende bijdragen'), pp(reconciliation.contribution_sum_pp)],
                [text('Monthly return · AIRS', 'Maandrendement · AIRS'), pct(reconciliation.monthly_return_pct)],
                [text('Unexplained difference', 'Onverklaard verschil'), pp(reconciliation.difference_pp)],
              ].map(([label, value]) => <tr key={label}><th colSpan={3} className="px-3 py-2 text-left font-medium">{label}</th><td className="px-3 py-2 text-right font-mono">{value}</td><td /></tr>)}
            </tfoot>}</table></div>}
        </div>
        <p role="status" className={`text-xs ${reconciliation?.status === 'reconciled' ? 'text-pos-400' : 'text-warn-300'}`}>{reconciliation?.status === 'reconciled' ? text('Reconciled with AIRS monthly return.', 'Sluit aan op het AIRS-maandrendement.') : text('Not reconciled: incomplete or inconsistent source data. No balancing adjustment has been applied.', 'Niet aangesloten: onvolledige of inconsistente brongegevens. Er is geen sluitpost toegepast.')}</p>
        <p className="text-[11px] leading-relaxed text-fg-faint">{text('Weight is the opening portfolio weight. Position return is the EUR price/FX change, excluding income. Contribution uses the actual EUR result adjusted for purchases and sales, divided by opening portfolio capital. For traded positions, opening weight × price return is not the contribution. Income and accrued interest are separate AIRS accounting rows, not allocated estimates. Exact contribution reconciliation is unavailable for months with external cash flows.', 'Gewicht is het begingewicht in de portefeuille. Positierendement is de koers-/valutaverandering in EUR, exclusief inkomsten. Bijdrage gebruikt het werkelijke EUR-resultaat, gecorrigeerd voor aan- en verkopen, gedeeld door het beginvermogen. Bij verhandelde posities is begingewicht × koersrendement niet de bijdrage. Inkomsten en opgelopen rente zijn afzonderlijke AIRS-boekhoudregels, geen geschatte toerekening. Exacte aansluiting van bijdragen is niet beschikbaar voor maanden met externe kasstromen.')}</p>
        </>}
      </div>}
      </div>
    </>}
  </section>;
}
