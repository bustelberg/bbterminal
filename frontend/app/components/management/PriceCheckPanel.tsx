'use client';

import { useLang } from '../../../lib/i18n';
import { isExtremeDifference, needsPriceReview, type PriceStatus, type PriceSummary } from './priceCheck';
import type { PriceCheckState } from './usePriceChecks';

const en = {
  column: 'Price check', checking: 'Checking…', failed: 'Check failed', retry: 'Force recheck', empty: 'No comparable prices',
  recheckHint: 'Fetch fresh AIRS data, refresh Yahoo prices and save a new check.',
  checkedOn: 'Last checked',
  forceAll: 'Force price checks', forceAllHint: 'Re-fetch AIRS data and refresh saved Yahoo comparisons for every analysable portfolio in this tab.',
  checked: (n: number, total: number) => `${n}/${total} checked`,
  differences: (n: number) => `${n} ${n === 1 ? 'difference' : 'differences'}`, stale: (n: number) => `${n} stale`,
  partial: 'Incomplete check', showIssues: 'Show affected portfolios', showAll: 'Show all portfolios',
  attention: (n: number) => `${n} portfolios need a price check`,
  alert: 'New price differences after refresh', dismiss: 'Dismiss alert',
  title: 'Price differences', holding: 'Holding', airs: 'AIRS', yahoo: 'Yahoo close',
  methodology: 'How this check works', subtitle: 'Native currency · gaps ≥5% only', dateDifference: 'Quote dates differ',
  delta: 'Difference', dates: 'AIRS / Yahoo dates', status: 'Status',
  matched: 'No extreme differences found in the available prices.',
  clear: 'No extreme gaps',
  range: '5-day close range', within: 'Possible timing difference', outside: 'Outside range · investigate', insufficient: 'Not enough recent history',
  note: 'Native prices are compared only in the same currency and quote unit, without FX conversion. Only gaps of 5% or more are shown and trigger alerts. Different dates are flagged as stale. Missing, linked or different-currency prices are not comparable; cash is excluded.',
  end: 'The range uses five stored trading-session closes before the AIRS report date, not intraday highs/lows or a statistical confidence interval. Being inside suggests timing, not proof. Checks use stored prices; Refresh updates the source data.',
  states: { matched: 'Matched', warning: 'Warning (>2%)', difference: 'Difference (≥5%)', stale: 'Different dates / stale', linked: 'Linked listing — not comparable', unavailable: 'Price unavailable' } satisfies Record<PriceStatus, string>,
};
type Copy = typeof en;
const nl: Copy = {
  column: 'Koerscontrole', checking: 'Controleren…', failed: 'Controle mislukt', retry: 'Opnieuw controleren', empty: 'Geen vergelijkbare koersen',
  recheckHint: 'Haal actuele AIRS-gegevens op, ververs Yahoo-koersen en sla een nieuwe controle op.',
  checkedOn: 'Laatst gecontroleerd',
  forceAll: 'Forceer koerscontroles', forceAllHint: 'Haal AIRS-gegevens opnieuw op en ververs opgeslagen Yahoo-vergelijkingen voor elke analyseerbare portefeuille in dit tabblad.',
  checked: (n, total) => `${n}/${total} gecontroleerd`, differences: (n) => `${n} ${n === 1 ? 'verschil' : 'verschillen'}`, stale: (n) => `${n} verouderd`,
  partial: 'Onvolledige controle', showIssues: 'Toon betrokken portefeuilles', showAll: 'Toon alle portefeuilles',
  attention: (n) => `${n} portefeuilles vereisen een koerscontrole`, alert: 'Nieuwe koersverschillen na verversen', dismiss: 'Melding sluiten',
  title: 'Koersverschillen', holding: 'Positie', airs: 'AIRS', yahoo: 'Yahoo-slotkoers',
  methodology: 'Hoe deze controle werkt', subtitle: 'Lokale valuta · alleen verschillen ≥5%', dateDifference: 'Koersdatums verschillen',
  delta: 'Verschil', dates: 'Datums AIRS / Yahoo', status: 'Status', matched: 'Geen extreme verschillen gevonden in de beschikbare koersen.',
  clear: 'Geen extreme verschillen',
  range: '5-daags slotkoersbereik', within: 'Mogelijk tijdsverschil', outside: 'Buiten bereik · onderzoeken', insufficient: 'Onvoldoende recente historie',
  note: 'Lokale koersen worden alleen vergeleken bij dezelfde valuta en koerseenheid, zonder valutaomrekening. Alleen verschillen vanaf 5% worden getoond en geven een melding. Andere datums gelden als verouderd. Ontbrekende, gekoppelde of verschillende valuta zijn niet vergelijkbaar; liquiditeiten tellen niet mee.',
  end: 'Het bereik gebruikt vijf opgeslagen slotkoersen vóór de AIRS-rapportdatum, geen intradagbereik of statistisch betrouwbaarheidsinterval. Binnen het bereik suggereert timing, maar bewijst niets. Verversen werkt de brongegevens bij.',
  states: { matched: 'Komt overeen', warning: 'Waarschuwing (>2%)', difference: 'Verschil (≥5%)', stale: 'Andere datums / verouderd', linked: 'Gekoppelde notering — niet vergelijkbaar', unavailable: 'Koers ontbreekt' },
};
export function usePriceCheckCopy() { const [lang] = useLang(); return lang === 'nl' ? nl : en; }

const tone = (summary: PriceSummary) => needsPriceReview(summary) ? 'text-neg-400 border-neg-500/30'
  : 'text-fg-subtle border-neutral-700';

export function PriceCheckBadge({ state, expanded, onClick }: { state?: PriceCheckState; expanded: boolean; onClick: () => void }) {
  const t = usePriceCheckCopy();
  const summary = state?.summary;
  const extremes = summary?.items.filter(isExtremeDifference).length ?? 0;
  if (!state) return null;
  if (state && !state.pending && !state.error && !extremes) return null;
  const label = !state || state.pending ? t.checking : state.error ? t.failed : !summary?.total ? t.empty
    : extremes ? t.differences(extremes) : t.clear;
  return <button type="button" aria-expanded={expanded} onClick={(event) => { event.stopPropagation(); onClick(); }}
    className={`rounded-md border px-2 py-1 text-xs whitespace-nowrap hover:bg-overlay/5 ${state?.error ? 'text-warn-400 border-warn-500/30' : state?.pending || !summary?.total ? 'text-fg-subtle border-neutral-700' : tone(summary)}`}
    title={`${t.title}${summary ? ` · ${t.checked(summary.checked, summary.total)}` : ''}`}>
    {label}
  </button>;
}

export function PriceCheckDetails({ state, onRetry, refreshing = false }: { state?: PriceCheckState; onRetry: () => void; refreshing?: boolean }) {
  const t = usePriceCheckCopy();
  const summary = state?.summary;
  const affected = summary?.items.filter(isExtremeDifference) ?? [];
  if (state && !state.pending && !state.error && !affected.length) return null;
  const price = (value: number | null) => value == null ? '—' : value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 });
  return <div className="rounded-lg border border-neutral-800/40 bg-card p-4 space-y-3">
    <div className="flex flex-wrap items-center justify-between gap-2">
      <div><h4 className="text-sm font-medium text-fg-strong">{t.title}</h4><p className="mt-1 text-xs text-fg-subtle">{t.subtitle}</p></div>
      <button type="button" onClick={onRetry} disabled={refreshing || state?.pending} aria-busy={refreshing || state?.pending} title={t.recheckHint}
        className="rounded-md border border-accent-500/30 px-3 py-1.5 text-xs text-accent-400 hover:bg-overlay/5 disabled:opacity-50">{refreshing || state?.pending ? t.checking : t.retry}</button>
    </div>
    {state?.checkedAt && <p className="text-xs text-fg-subtle">{t.checkedOn}: {new Date(state.checkedAt).toLocaleString()}</p>}
    {state?.error && <p role="alert" className="text-xs text-warn-400">{t.failed}</p>}
    {!state || state.pending ? <p className="text-xs text-fg-subtle">{t.checking}</p> : affected.length > 0 ?
      <div className="overflow-auto"><table className="w-full whitespace-nowrap text-xs">
        <thead><tr className="border-b border-neutral-800/40 text-fg-subtle">
          {[t.holding, t.airs, t.yahoo, t.delta, t.range, t.status].map((label, index) => <th key={label} className={`px-3 py-2 font-medium ${index > 0 && index < 4 ? 'text-right' : 'text-left'}`}>{label}</th>)}
        </tr></thead>
        <tbody>{affected.map((row) => <tr key={row.key} className="border-b border-neutral-800/30 text-fg-muted">
          <td className="px-3 py-3"><span className="block font-medium text-fg-strong">{row.name}</span><span className="mt-1 block font-mono text-fg-subtle">{row.isin ?? '—'}</span>{row.instrument && row.instrument !== row.name && <span className="mt-1 block max-w-64 whitespace-normal text-fg-subtle">Yahoo: {row.instrument}</span>}</td>
          <td className="px-3 py-3 text-right"><span className="font-mono tabular-nums">{price(row.airs)} {row.airsCurrency}</span><span className="mt-1 block text-fg-subtle">{row.airsDate ?? '—'}</span></td>
          <td className="px-3 py-3 text-right"><span className="font-mono tabular-nums">{price(row.yahoo)} {row.yahooCurrency}</span><span className="mt-1 block text-fg-subtle">{row.yahooDate ?? '—'}</span></td>
          <td className="px-3 py-3 text-right font-mono tabular-nums font-medium text-neg-400" title="(AIRS price / Yahoo price − 1) × 100">{row.difference == null || row.status === 'linked' ? '—' : `${row.difference >= 0 ? '+' : ''}${row.difference.toFixed(2)}%`}</td>
          <td className="px-3 py-3">{row.recentRange ? <><span className="font-mono tabular-nums">{price(row.recentRange.low)}–{price(row.recentRange.high)} {row.yahooCurrency}</span><span className="mt-1 block text-fg-subtle">{row.recentRange.start} → {row.recentRange.end}{row.recentRange.count < 5 ? ` · ${row.recentRange.count}/5` : ''}</span></> : '—'}</td>
          <td className="px-3 py-3"><span className={`inline-block rounded-md px-2 py-1 ${row.rangeAssessment === 'outside' ? 'bg-neg-500/10 text-neg-400' : 'bg-warn-500/10 text-warn-400'}`}>{t[row.rangeAssessment]}</span>{row.airsDate !== row.yahooDate && <span className="mt-1 block text-fg-subtle">{t.dateDifference}</span>}</td>
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
