'use client';

import { useLang } from '../../../lib/i18n';
import { isExtremeDifference, needsPriceReview, type PriceStatus, type PriceSummary } from './priceCheck';
import type { PriceCheckState } from './usePriceChecks';

const en = {
  column: 'Price check', checking: 'Checking…', viewPrices: 'View prices', viewPricesHint: 'Show the saved AIRS and Yahoo prices for every listed holding.', failed: 'Check failed', retry: 'Force recheck', empty: 'No comparable prices',
  recheckHint: 'Fetch fresh AIRS data, refresh Yahoo prices and save a new check.',
  checkedOn: 'Last checked',
  forceAll: 'Force price checks', forceAllHint: 'Re-fetch AIRS data and refresh saved Yahoo comparisons for every analysable portfolio in this tab.',
  checked: (n: number, total: number) => `${n}/${total} checked`,
  differences: (n: number) => `${n} ${n === 1 ? 'difference' : 'differences'}`, stale: (n: number) => `${n} stale`,
  partial: 'Incomplete check', showIssues: 'Show affected portfolios', showAll: 'Show all portfolios',
  attention: (n: number) => `${n} portfolios need a price check`, close: 'Close',
  alert: 'New price differences after refresh', dismiss: 'Dismiss alert',
  title: 'Price differences', allTitle: 'Price comparison', holding: 'Holding', certificate: 'Certificate', airs: 'AIRS', yahoo: 'Yahoo close',
  methodology: 'How this check works', subtitle: 'Listed stocks and ETFs · gaps ≥5% only', dateDifference: 'Quote dates differ',
  delta: 'Difference', dates: 'AIRS / Yahoo dates', status: 'Status', recheck: 'Recheck', rechecking: 'Rechecking…',
  matched: 'No extreme differences found in the available prices.',
  clear: 'No extreme gaps',
  range: '5-day close range', within: 'Possible timing difference', outside: 'Outside range · investigate', insufficient: 'Not enough recent history', notComparable: 'Not comparable', staleRange: 'Price range is stale',
  note: 'Only listed stocks and ETFs are shown. Native prices are compared only in the same currency and quote unit, without FX conversion. Only gaps of 5% or more are shown and trigger alerts. Different dates are flagged as stale.',
  end: 'The range uses five stored trading-session closes before the AIRS report date, not intraday highs/lows or a statistical confidence interval. Being inside suggests timing, not proof. Checks use stored prices; Refresh updates the source data.',
  states: { matched: 'Matched', warning: 'Warning (>2%)', difference: 'Difference (≥5%)', stale: 'Different dates / stale', linked: 'Linked listing — not comparable', unavailable: 'Price unavailable' } satisfies Record<PriceStatus, string>,
};
type Copy = typeof en;
const nl: Copy = {
  column: 'Koerscontrole', checking: 'Controleren…', viewPrices: 'Koersen bekijken', viewPricesHint: 'Toon de opgeslagen AIRS- en Yahoo-koersen voor elke beursgenoteerde positie.', failed: 'Controle mislukt', retry: 'Opnieuw controleren', empty: 'Geen vergelijkbare koersen',
  recheckHint: 'Haal actuele AIRS-gegevens op, ververs Yahoo-koersen en sla een nieuwe controle op.',
  checkedOn: 'Laatst gecontroleerd',
  forceAll: 'Forceer koerscontroles', forceAllHint: 'Haal AIRS-gegevens opnieuw op en ververs opgeslagen Yahoo-vergelijkingen voor elke analyseerbare portefeuille in dit tabblad.',
  checked: (n, total) => `${n}/${total} gecontroleerd`, differences: (n) => `${n} ${n === 1 ? 'verschil' : 'verschillen'}`, stale: (n) => `${n} verouderd`,
  partial: 'Onvolledige controle', showIssues: 'Toon betrokken portefeuilles', showAll: 'Toon alle portefeuilles',
  attention: (n) => `${n} portefeuilles vereisen een koerscontrole`, close: 'Sluiten', alert: 'Nieuwe koersverschillen na verversen', dismiss: 'Melding sluiten',
  title: 'Koersverschillen', allTitle: 'Koersvergelijking', holding: 'Positie', certificate: 'Certificaat', airs: 'AIRS', yahoo: 'Yahoo-slotkoers',
  methodology: 'Hoe deze controle werkt', subtitle: 'Beursgenoteerde aandelen en ETF’s · alleen verschillen ≥5%', dateDifference: 'Koersdatums verschillen',
  delta: 'Verschil', dates: 'Datums AIRS / Yahoo', status: 'Status', recheck: 'Opnieuw controleren', rechecking: 'Controleren…', matched: 'Geen extreme verschillen gevonden in de beschikbare koersen.',
  clear: 'Geen extreme verschillen',
  range: '5-daags slotkoersbereik', within: 'Mogelijk tijdsverschil', outside: 'Buiten bereik · onderzoeken', insufficient: 'Onvoldoende recente historie', notComparable: 'Niet vergelijkbaar', staleRange: 'Koersbereik is verouderd',
  note: 'Alleen beursgenoteerde aandelen en ETF’s worden getoond. Lokale koersen worden alleen vergeleken bij dezelfde valuta en koerseenheid, zonder valutaomrekening. Alleen verschillen vanaf 5% worden getoond en geven een melding. Andere datums gelden als verouderd.',
  end: 'Het bereik gebruikt vijf opgeslagen slotkoersen vóór de AIRS-rapportdatum, geen intradagbereik of statistisch betrouwbaarheidsinterval. Binnen het bereik suggereert timing, maar bewijst niets. Verversen werkt de brongegevens bij.',
  states: { matched: 'Komt overeen', warning: 'Waarschuwing (>2%)', difference: 'Verschil (≥5%)', stale: 'Andere datums / verouderd', linked: 'Gekoppelde notering — niet vergelijkbaar', unavailable: 'Koers ontbreekt' },
};
export function usePriceCheckCopy() { const [lang] = useLang(); return lang === 'nl' ? nl : en; }

const tone = (summary: PriceSummary) => needsPriceReview(summary) ? 'text-neg-400 border-neg-500/30'
  : 'text-fg-subtle border-neutral-700';

function yahooQuoteUrl(symbol: string) {
  return `https://finance.yahoo.com/quote/${encodeURIComponent(symbol)}/`;
}

export function PriceCheckBadge({ state, onClick }: { state?: PriceCheckState; onClick: () => void }) {
  const t = usePriceCheckCopy();
  const summary = state?.summary;
  const extremes = summary?.items.filter(isExtremeDifference).length ?? 0;
  if (!state) return null;
  if (state && !state.pending && !state.error && !extremes) return null;
  const label = !state || state.pending ? t.checking : state.error ? t.failed : !summary?.total ? t.empty
    : extremes ? t.differences(extremes) : t.clear;
  return <button type="button" aria-haspopup="dialog" onClick={(event) => { event.stopPropagation(); onClick(); }}
    className={`rounded-md border px-2 py-1 text-xs whitespace-nowrap hover:bg-overlay/5 ${state?.error ? 'text-warn-400 border-warn-500/30' : state?.pending || !summary?.total ? 'text-fg-subtle border-neutral-700' : tone(summary)}`}
    title={`${t.title}${summary ? ` · ${t.checked(summary.checked, summary.total)}` : ''}`}>
    {label}
  </button>;
}

export function PriceCheckModal({ name, state, onRetry, onRecheckRow, recheckingIsins, refreshing = false, showAll = false, onClose }: {
  name: string; state?: PriceCheckState; onRetry: () => void; onRecheckRow?: (isin: string, name: string) => void;
  recheckingIsins?: ReadonlySet<string>; refreshing?: boolean; showAll?: boolean; onClose: () => void;
}) {
  const t = usePriceCheckCopy();
  return <div className="fixed inset-0 z-[90] flex items-center justify-center bg-scrim/60 p-4" role="dialog" aria-modal="true"
    aria-labelledby="price-check-modal-title" onClick={onClose}>
    <div className="max-h-[85vh] w-full min-[640px]:w-[80vw] max-w-[80vw] overflow-auto rounded-xl border border-neutral-800/40 bg-page p-5 shadow-xl"
      onClick={(event) => event.stopPropagation()}>
      <div className="mb-4 flex items-start justify-between gap-4">
        <div className="min-w-0"><h2 id="price-check-modal-title" className="text-base font-semibold text-fg-strong">{name}</h2>
          <p className="mt-0.5 text-xs text-fg-subtle">{showAll ? t.allTitle : t.title}</p></div>
        <button type="button" onClick={onClose} className="shrink-0 rounded-md border border-neutral-700 px-3 py-1.5 text-xs text-fg-muted hover:border-accent-500/50 hover:text-accent-300">{t.close}</button>
      </div>
      <PriceCheckDetails state={state} refreshing={refreshing} onRetry={onRetry} showAll={showAll}
        onRecheckRow={onRecheckRow} recheckingIsins={recheckingIsins} />
    </div>
  </div>;
}

export function PriceCheckDetails({ state, onRetry, onRecheckRow, recheckingIsins, refreshing = false, showAll = false }: {
  state?: PriceCheckState; onRetry: () => void; onRecheckRow?: (isin: string, name: string) => void;
  recheckingIsins?: ReadonlySet<string>; refreshing?: boolean; showAll?: boolean;
}) {
  const t = usePriceCheckCopy();
  const summary = state?.summary;
  const affected = summary?.items.filter(isExtremeDifference) ?? [];
  const displayed = showAll ? summary?.items ?? [] : affected;
  // “View prices” must still explain a bond-only account rather than opening a modal whose body
  // disappears after the stock/ETF eligibility filter leaves no rows.
  if (state && !state.pending && !state.error && !displayed.length && !showAll) return null;
  const price = (value: number | null) => value == null ? '—' : value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 });
  return <div className="rounded-lg border border-neutral-800/40 bg-card p-4 space-y-3">
    <div className="flex flex-wrap items-center justify-between gap-2">
      <div><h4 className="text-sm font-medium text-fg-strong">{showAll ? t.allTitle : t.title}</h4><p className="mt-1 text-xs text-fg-subtle">{showAll ? t.note : t.subtitle}</p></div>
      <button type="button" onClick={onRetry} disabled={refreshing || state?.pending} aria-busy={refreshing || state?.pending} title={t.recheckHint}
        className="rounded-md border border-accent-500/30 px-3 py-1.5 text-xs text-accent-400 hover:bg-overlay/5 disabled:opacity-50">{refreshing || state?.pending ? t.checking : t.retry}</button>
    </div>
    {state?.checkedAt && <p className="text-xs text-fg-subtle">{t.checkedOn}: {new Date(state.checkedAt).toLocaleString()}</p>}
    {state?.error && <p role="alert" className="text-xs text-warn-400">{t.failed}</p>}
    {/* Keep the existing table mounted while its replacement snapshot is loading. Replacing it
        with a short “Checking…” line collapses the scroll container and sends the reader to top. */}
    {!state || (state.pending && !summary) ? <p className="text-xs text-fg-subtle">{t.checking}</p> : displayed.length > 0 ?
      <div className="overflow-auto"><table className="w-full whitespace-nowrap text-xs">
        <thead><tr className="border-b border-neutral-800/40 text-fg-subtle">
          {[t.holding, t.certificate, t.airs, t.yahoo, t.delta, t.range, t.status, ''].map((label, index) => <th key={`${label}-${index}`} className={`px-3 py-2 font-medium ${index === 7 ? 'w-28 text-right' : index > 1 && index < 5 ? 'text-right' : 'text-left'}`}>{label}</th>)}
        </tr></thead>
        <tbody>{displayed.map((row) => <tr key={row.key} className="group border-b border-neutral-800/30 text-fg-muted hover:bg-overlay/5">
          <td className="min-w-60 px-3 py-3 font-medium text-fg-strong">{row.yahooSymbol ? <a href={yahooQuoteUrl(row.yahooSymbol)} target="_blank" rel="noreferrer" title={`Open ${row.yahooSymbol} on Yahoo Finance`} className="hover:text-accent-400 hover:underline">{row.name} <span aria-hidden="true">↗</span></a> : row.name}</td>
          <td className="max-w-48 px-3 py-3 whitespace-normal text-fg-subtle">{row.certificate ?? '—'}</td>
          <td className="px-3 py-3 text-right"><span className="font-mono tabular-nums">{price(row.airs)} {row.airsCurrency}</span><span className="mt-1 block text-fg-subtle">{row.airsDate ?? '—'}</span></td>
          <td className="px-3 py-3 text-right"><span className="font-mono tabular-nums">{price(row.yahoo)} {row.yahooCurrency}</span><span className="mt-1 block text-fg-subtle">{row.yahooDate ?? '—'}</span></td>
          <td className={`px-3 py-3 text-right font-mono tabular-nums font-medium ${isExtremeDifference(row) ? 'text-neg-400' : 'text-fg-muted'}`} title="(AIRS price / Yahoo price − 1) × 100">{row.difference == null || row.status === 'linked' ? '—' : (() => { const difference = Math.abs(row.difference) < 0.005 ? 0 : row.difference; return `${difference > 0 ? '+' : ''}${difference.toFixed(2)}%`; })()}</td>
          <td className="px-3 py-3">{row.recentRange ? <><span className="font-mono tabular-nums">{price(row.recentRange.low)}–{price(row.recentRange.high)} {row.yahooCurrency}</span><span className="mt-1 block text-fg-subtle">{row.recentRange.start} → {row.recentRange.end}{row.recentRange.count < 5 ? ` · ${row.recentRange.count}/5` : ''}</span></> : '—'}</td>
          <td className={`px-3 py-3 ${row.rangeAssessment === 'insufficient' ? 'text-center' : ''}`}><span className={`inline-block rounded-md px-2 py-1 ${row.rangeAssessment === 'outside' ? 'bg-neg-500/10 text-neg-400' : row.rangeAssessment === 'notComparable' ? 'bg-neutral-700/30 text-fg-subtle' : 'bg-warn-500/10 text-warn-400'}`}>{t[row.rangeAssessment]}</span>{row.airsDate !== row.yahooDate && <span className="mt-1 block text-fg-subtle">{t.dateDifference}</span>}</td>
          <td className="w-28 px-3 py-3 text-right">{row.isin && onRecheckRow && <button type="button"
            disabled={recheckingIsins?.has(row.isin)} onClick={() => onRecheckRow(row.isin!, row.name)}
            title={t.recheck} className="w-24 rounded-md border border-neutral-700 px-2 py-1 text-xs text-fg-subtle opacity-0 transition-opacity hover:border-accent-500/50 hover:text-accent-300 focus:opacity-100 group-hover:opacity-100 disabled:cursor-not-allowed disabled:opacity-50">
            {recheckingIsins?.has(row.isin) ? t.rechecking : t.recheck}
          </button>}</td>
        </tr>)}</tbody>
      </table></div> : !state?.error && <p className="text-xs text-fg-subtle">{summary?.total ? t.matched : t.empty}</p>}
    <details className="max-w-4xl text-xs text-fg-subtle"><summary className="w-fit cursor-pointer hover:text-fg-strong">{t.methodology}</summary><p className="mt-2 leading-relaxed">{t.note} {t.end}</p></details>
  </div>;
}

export function PriceCheckNotice({ alerts, names, onDismiss }: {
  alerts: Record<string, number>; names: Record<string, string>; onDismiss: () => void;
}) {
  const t = usePriceCheckCopy();
  const accounts = Object.keys(names);
  const fresh = accounts.filter((account) => alerts[account]);
  if (!fresh.length) return null;
  return <div role="alert" className="flex flex-wrap items-center gap-3 rounded-lg border border-warn-500/30 bg-warn-500/10 px-3 py-2 text-xs text-warn-400">
    <span className="font-medium">{t.alert}: {fresh.map((account) => `${names[account]} (${alerts[account]})`).join(', ')}</span><button type="button" onClick={onDismiss} className="underline">{t.dismiss}</button>
  </div>;
}
