'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { useLang } from '../../../lib/i18n';
import { colorForSector } from '../../../lib/sectorColors';
import type { EtfSectorAllocationResponse, EtfRegionAllocationResponse, EtfCurrencyAllocationResponse } from '../../../lib/types/api';
import PanelDialog from './PanelDialog';
import LoadingDots from './LoadingDots';
import {
  collapseEtfSectors, etfSectorPortfolioContribution, normalizedEtfSectorWeight,
} from './etfSectorLookThrough';

export default function EtfSectorAllocationModal({
  isin, name, portfolioWeightPct, initialData, onClose,
}: {
  isin?: string; name: string; portfolioWeightPct: number;
  initialData?: EtfSectorAllocationResponse; onClose: () => void;
}) {
  const [lang] = useLang();
  const [sectorData, setData] = useState<EtfSectorAllocationResponse | null>(initialData ?? null);
  const [sectorError, setError] = useState<string | null>(null);
  const [regionData, setRegionData] = useState<EtfRegionAllocationResponse | null>(null);
  const [regionError, setRegionError] = useState<string | null>(null);
  const [currencyData, setCurrencyData] = useState<EtfCurrencyAllocationResponse | null>(null);
  const [currencyError, setCurrencyError] = useState<string | null>(null);
  const tabs = ['sector', 'region', 'currency'] as const;
  const [tab, setTab] = useState<typeof tabs[number]>('sector');

  useEffect(() => {
    if (initialData || !isin) return undefined;
    const controller = new AbortController();
    void (async () => {
      try {
        const response = await apiFetch(
          `${API_URL}/api/airs/etf/${encodeURIComponent(isin)}/sector-allocation`,
          { signal: controller.signal },
        );
        const body = await response.json().catch(() => null);
        if (!response.ok) throw new Error(body?.detail ?? `HTTP ${response.status}`);
        setData(body as EtfSectorAllocationResponse);
      } catch (reason) {
        if (controller.signal.aborted) return;
        setError(reason instanceof Error ? reason.message : String(reason));
      }
    })();
    return () => controller.abort();
  }, [initialData, isin]);

  const regional = regionData?.isin === isin?.trim().toUpperCase() ? regionData : null;
  useEffect(() => {
    if (tab !== 'region' || !isin || regional) return;
    const controller = new AbortController();
    setRegionError(null);
    void (async () => {
      try {
        const response = await apiFetch(`${API_URL}/api/airs/etf/${encodeURIComponent(isin)}/region-allocation`, { signal: controller.signal });
        const body = await response.json().catch(() => null);
        if (!response.ok) throw new Error(body?.detail ?? `HTTP ${response.status}`);
        if (!controller.signal.aborted) setRegionData(body as EtfRegionAllocationResponse);
      } catch (reason) {
        if (!controller.signal.aborted) setRegionError(reason instanceof Error ? reason.message : String(reason));
      }
    })();
    return () => controller.abort();
  }, [tab, isin, regional]);

  const currencies = currencyData?.isin === isin?.trim().toUpperCase() ? currencyData : null;
  useEffect(() => {
    if (tab !== 'currency' || !isin || currencies) return;
    const controller = new AbortController();
    setCurrencyError(null);
    void (async () => {
      try {
        const response = await apiFetch(`${API_URL}/api/airs/etf/${encodeURIComponent(isin)}/currency-allocation`, { signal: controller.signal });
        const body = await response.json().catch(() => null);
        if (!response.ok) throw new Error(body?.detail ?? `HTTP ${response.status}`);
        if (!controller.signal.aborted) setCurrencyData(body as EtfCurrencyAllocationResponse);
      } catch (reason) {
        if (!controller.signal.aborted) setCurrencyError(reason instanceof Error ? reason.message : String(reason));
      }
    })();
    return () => controller.abort();
  }, [tab, isin, currencies]);
  const currencyGroups = new Map<string, { sector: string; weight_pct: number }[]>();
  for (const holding of currencies?.holdings ?? []) {
    const group = currencyGroups.get(holding.currency) ?? [];
    group.push({ sector: `${holding.name}${holding.symbol ? ` · ${holding.symbol}` : ''}`, weight_pct: holding.weight_pct });
    currencyGroups.set(holding.currency, group);
  }
  const currencyTable = currencies ? { ...currencies, sectors: [...currencyGroups].map(([currency, holdings]) => ({
    sector: currency, weight_pct: holdings.reduce((sum, row) => sum + row.weight_pct, 0),
    provider_sectors: holdings.map(row => row.sector), provider_weights: holdings,
  })) } : null;

  // The same allocation table renders either taxonomy; source labels and weights stay intact.
  const data = tab === 'currency' ? currencyTable : tab === 'sector' ? sectorData : regional ? { ...regional, sectors: regional.regions.map(row => ({
    sector: row.region, weight_pct: row.weight_pct, provider_sectors: row.provider_weights.map(item => item.country),
    provider_weights: row.provider_weights.map(item => ({ sector: item.country, weight_pct: item.weight_pct })),
  })) } : null;
  const error = tab === 'currency' ? currencyError : tab === 'sector' ? sectorError : regionError;

  const date = data?.as_of
    ? new Intl.DateTimeFormat(lang === 'nl' ? 'nl-NL' : 'en-GB', {
      day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC',
    }).format(new Date(`${data.as_of}T00:00:00Z`))
    : null;
  const sectorLabels = lang === 'nl'
    ? { title: 'Sectorverdeling', asOf: 'Per', source: 'Bron', close: 'Sluiten',
      providerSector: 'Oorspronkelijke sector', originalWeight: 'Origineel',
      ourSector: 'Onze sector', ourWeight: 'Onze fondsweging', currentWeight: 'Actueel gewicht',
      portfolioWeight: 'In portefeuille',
      total: 'Totaal',
      holdingWeight: (weight: string) => `Dit fonds is ${weight}% van de actuele portefeuille.`,
      loading: 'Sectorwegingen laden', failed: 'Sectorverdeling kon niet worden geladen.' }
    : { title: 'Sector allocation', asOf: 'As of', source: 'Source', close: 'Close',
      providerSector: 'Original sector', originalWeight: 'Original weight',
      ourSector: 'Our sector', ourWeight: 'Our fund weight', currentWeight: 'Current weight',
      portfolioWeight: 'In portfolio',
      total: 'Total',
      holdingWeight: (weight: string) => `This fund is ${weight}% of the current portfolio.`,
      loading: 'Loading sector weights', failed: 'Sector allocation could not be loaded.' };
  const labels = tab === 'currency' ? { ...sectorLabels,
    title: lang === 'nl' ? 'Valutaverdeling' : 'Currency allocation',
    providerSector: currencies?.basis === 'published_currency_allocation'
      ? (lang === 'nl' ? 'Gepubliceerde verdeling' : 'Published allocation')
      : (lang === 'nl' ? 'Onderliggende positie' : 'Underlying holding'),
    ourSector: lang === 'nl' ? 'Noteringsvaluta' : 'Quote currency',
    loading: lang === 'nl' ? 'Valuta laden' : 'Loading holding currencies',
    failed: lang === 'nl' ? 'Valutaverdeling kon niet worden geladen.' : 'Currency allocation could not be loaded.',
  } : tab === 'sector' ? sectorLabels : { ...sectorLabels,
    title: lang === 'nl' ? 'Regioverdeling' : 'Region allocation',
    providerSector: lang === 'nl' ? 'Oorspronkelijk land' : 'Original country',
    ourSector: lang === 'nl' ? 'Onze regio' : 'Our region',
    loading: lang === 'nl' ? 'Regiowegingen laden' : 'Loading region weights',
    failed: lang === 'nl' ? 'Regioverdeling kon niet worden geladen.' : 'Region allocation could not be loaded.',
  };
  const mappedSectors = tab === 'sector' ? collapseEtfSectors(data?.sectors ?? []) : data?.sectors ?? [];
  const sourceTotal = mappedSectors.reduce((sum, row) => sum + row.weight_pct, 0);
  const ourTotal = mappedSectors.reduce(
    (sum, row) => sum + normalizedEtfSectorWeight(row.weight_pct, sourceTotal), 0,
  );

  return (
    <PanelDialog onClose={onClose} labelledBy="etf-sector-allocation-title">
      <div className="h-full min-h-0 flex flex-col rounded-xl border border-neutral-800/40 bg-card p-4">
        <div className="shrink-0 flex items-start justify-between gap-3 border-b border-neutral-800/30 pb-3">
          <div className="min-w-0">
            <h4 id="etf-sector-allocation-title" className="text-sm font-semibold text-fg-strong">
              {labels.title}
            </h4>
            <p className="mt-0.5 truncate text-xs text-fg-muted" title={name}>{name}</p>
            {isin && <p className="mt-1 font-mono text-[11px] text-fg-faint">{isin}</p>}
            <p className="mt-1 text-[11px] text-fg-faint">
              {labels.holdingWeight(portfolioWeightPct.toFixed(2))}
            </p>
            {data && data.source_url ? (
              <a href={data.source_url} target="_blank" rel="noreferrer"
                className="mt-1 inline-flex text-[11px] text-accent-300 hover:text-accent-200 hover:underline">
                {labels.source}: {data.source}
              </a>
            ) : data ? (
              <p className="mt-1 text-[11px] text-fg-faint">{labels.source}: {data.source}</p>
            ) : null}
          </div>
          <button type="button" onClick={onClose}
            className="cursor-pointer rounded-lg border border-neutral-700 px-3 py-1.5 text-xs text-fg-muted hover:border-accent-500/50 hover:text-accent-300">
            {labels.close}
          </button>
        </div>

        <div role="tablist" aria-label={lang === 'nl' ? 'Fondsverdeling' : 'Fund allocation'}
          className="mt-4 inline-flex w-fit shrink-0 rounded-lg border border-neutral-800/40 text-xs">
          {tabs.map((key) => <button key={key} type="button" role="tab"
            id={`etf-allocation-tab-${key}`} aria-selected={tab === key} aria-controls={`etf-allocation-panel-${key}`}
            tabIndex={tab === key ? 0 : -1} onClick={() => setTab(key)}
            onKeyDown={(event) => {
              const next = event.key === 'Home' ? tabs[0] : event.key === 'End' ? tabs[2]
                : event.key === 'ArrowLeft' ? tabs[(tabs.indexOf(key) + 2) % 3]
                : event.key === 'ArrowRight' ? tabs[(tabs.indexOf(key) + 1) % 3] : null;
              if (next) { event.preventDefault(); setTab(next); document.getElementById(`etf-allocation-tab-${next}`)?.focus(); }
            }} className={`px-3 py-1.5 font-medium transition-colors first:rounded-l-lg last:rounded-r-lg ${tab === key ? 'bg-accent-600 text-white' : 'text-fg-subtle hover:bg-overlay/5'}`}>
            {key === 'sector' ? 'Sector' : key === 'currency' ? (lang === 'nl' ? 'Valuta' : 'Currency') : lang === 'nl' ? 'Regio' : 'Region'}
          </button>)}
        </div>

        {tabs.map(key => <div key={key} role="tabpanel" id={`etf-allocation-panel-${key}`} aria-labelledby={`etf-allocation-tab-${key}`}
          hidden={tab !== key} className="min-h-0 flex-1 overflow-auto py-4">{tab === key && <>
          {tab === 'currency' && <p className="mb-3 text-xs text-fg-muted">{currencies?.basis === 'published_currency_allocation'
            ? (lang === 'nl' ? 'Gepubliceerde valutaverdeling van het fonds. Overige en onbekende valuta worden als Overig getoond. De bron vermeldt geen verdelingsdatum.'
              : 'Published fund currency allocation. Other and unknown currencies are shown as Other. The source does not provide an allocation date.')
            : (lang === 'nl' ? 'Noteringsvaluta per gepubliceerde positie, zonder valuta-afdekking. Niet-gepubliceerde posities en onbekende valuta worden als Overig getoond. Yahoo vermeldt geen datum voor deze posities.'
              : 'Quote currencies of published holdings, before hedging. Unreported holdings and unknown currencies are shown as Other. Yahoo does not provide a holdings date.')}</p>}
          {!data && !error && (
            <div className="flex h-full items-center justify-center text-sm text-fg-muted">
              {labels.loading}<LoadingDots />
            </div>
          )}
          {error && (
            <div className="rounded-lg border border-neg-500/20 bg-neg-500/10 px-3 py-2 text-xs text-neg-300">
              <p className="font-medium">{labels.failed}</p>
              <p className="mt-1 text-neg-300/80">{error}</p>
            </div>
          )}
          {data && (
            <div className="mx-auto max-w-5xl">
              <div className="mb-2 grid grid-cols-[minmax(9rem,1.2fr)_5.25rem_1.5rem_minmax(9rem,1.2fr)_minmax(6rem,1fr)_5.25rem_5.75rem_5.75rem] items-end gap-3 border-b border-neutral-800/30 pb-2 text-[10px] font-medium uppercase tracking-wide text-fg-faint">
                <span>{data.source} · {labels.providerSector}</span>
                <span className="text-right">{labels.originalWeight}</span>
                <span aria-hidden />
                <span>{labels.ourSector}</span>
                <span aria-hidden />
                <span className="text-right">{labels.ourWeight}</span>
                <span className="text-right">{labels.currentWeight}</span>
                <span className="text-right">{labels.portfolioWeight}</span>
              </div>
              <div className="space-y-2">
              {mappedSectors.map((row) => {
                // The chart uses this normalized figure too: provider tables can publish 99.99%
                // or 100.01% after rounding, but one ETF must contribute exactly its held weight.
                const ourWeight = normalizedEtfSectorWeight(row.weight_pct, sourceTotal);
                const portfolioContribution = etfSectorPortfolioContribution(
                  portfolioWeightPct, row.weight_pct, sourceTotal,
                );
                return (
                <div key={row.sector} className="grid grid-cols-[minmax(9rem,1.2fr)_5.25rem_1.5rem_minmax(9rem,1.2fr)_minmax(6rem,1fr)_5.25rem_5.75rem_5.75rem] items-center gap-3 text-xs">
                  <span className="min-w-0 text-fg-muted"
                    title={row.provider_sectors.join(' + ')}>
                    {row.provider_weights.map((provider) => (
                      <span key={provider.sector} className="block truncate">{provider.sector}</span>
                    ))}
                  </span>
                  <span className="font-mono text-right tabular-nums text-fg-muted">
                    {row.provider_weights.map((provider) => (
                      <span key={provider.sector} className="block">
                        {provider.weight_pct.toFixed(2)}%
                      </span>
                    ))}
                  </span>
                  <span className="text-center text-base text-fg-faint" aria-label="maps to">→</span>
                  <span className="flex min-w-0 items-center gap-2 font-medium text-fg-strong"
                    title={row.sector === 'Unclassified' ? (lang === 'nl' ? 'Overig' : 'Other') : row.sector}>
                    <span className="h-2.5 w-2.5 shrink-0 rounded-full"
                      style={{ background: colorForSector(row.sector) }} aria-hidden />
                    <span className="truncate">{row.sector === 'Unclassified' ? (lang === 'nl' ? 'Overig' : 'Other') : row.sector}</span>
                  </span>
                  <span className="relative h-3 overflow-hidden rounded-sm bg-overlay/[0.06]" aria-hidden>
                    <span className="absolute inset-y-0 left-0 rounded-sm"
                      style={{ width: `${Math.min(100, ourWeight)}%`, background: chartTheme.accent }} />
                  </span>
                  <span className="text-right font-mono tabular-nums text-fg-strong">
                    {ourWeight.toFixed(2)}%
                  </span>
                  <span className="text-right font-mono tabular-nums text-fg-muted">
                    {portfolioWeightPct.toFixed(2)}%
                  </span>
                  <span className="text-right font-mono tabular-nums text-accent-300"
                    title={`${ourWeight.toFixed(2)}% × ${portfolioWeightPct.toFixed(2)}% = ${portfolioContribution.toFixed(2)}%`}>
                    {portfolioContribution.toFixed(2)}%
                  </span>
                </div>
                );
              })}
              </div>
              <div className="mt-3 grid grid-cols-[minmax(9rem,1.2fr)_5.25rem_1.5rem_minmax(9rem,1.2fr)_minmax(6rem,1fr)_5.25rem_5.75rem_5.75rem] items-center gap-3 border-t border-neutral-800/40 pt-2 text-xs font-semibold">
                <span className="text-fg-muted">{labels.total}</span>
                <span className="text-right font-mono tabular-nums text-fg-strong">
                  {sourceTotal.toFixed(2)}%
                </span>
                <span aria-hidden />
                <span className="text-fg-muted">{labels.total}</span>
                <span aria-hidden />
                <span className="text-right font-mono tabular-nums text-fg-strong">
                  {ourTotal.toFixed(2)}%
                </span>
                <span aria-hidden />
                <span aria-hidden />
              </div>
            </div>
          )}
        </>}</div>)}

        {data && date && (
          <div className="shrink-0 border-t border-neutral-800/30 pt-3 text-[11px] text-fg-faint">
            <span>{labels.asOf} {date}</span>
          </div>
        )}
      </div>
    </PanelDialog>
  );
}
