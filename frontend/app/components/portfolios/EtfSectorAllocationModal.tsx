'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { useLang } from '../../../lib/i18n';
import { colorForSector } from '../../../lib/sectorColors';
import type { EtfSectorAllocationResponse } from '../../../lib/types/api';
import PanelDialog from './PanelDialog';
import LoadingDots from './LoadingDots';
import {
  collapseEtfSectors, etfSectorPortfolioContribution, normalizedEtfSectorWeight,
} from './etfSectorLookThrough';

export default function EtfSectorAllocationModal({ isin, name, portfolioWeightPct, onClose }: {
  isin: string; name: string; portfolioWeightPct: number; onClose: () => void;
}) {
  const [lang] = useLang();
  const [data, setData] = useState<EtfSectorAllocationResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
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
  }, [isin]);

  const date = data?.as_of
    ? new Intl.DateTimeFormat(lang === 'nl' ? 'nl-NL' : 'en-GB', {
      day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC',
    }).format(new Date(`${data.as_of}T00:00:00Z`))
    : null;
  const labels = lang === 'nl'
    ? { title: 'Sectorverdeling', asOf: 'Per', source: 'Bron', close: 'Sluiten',
      providerSector: 'Oorspronkelijke sector', originalWeight: 'Origineel',
      ourSector: 'Onze sector', ourWeight: 'Onze ETF-weging', currentWeight: 'Actueel gewicht',
      portfolioWeight: 'In portefeuille',
      holdingWeight: (weight: string) => `Deze ETF is ${weight}% van de actuele portefeuille.`,
      loading: 'Sectorwegingen laden', failed: 'Sectorverdeling kon niet worden geladen.' }
    : { title: 'Sector allocation', asOf: 'As of', source: 'Source', close: 'Close',
      providerSector: 'Original sector', originalWeight: 'Original weight',
      ourSector: 'Our sector', ourWeight: 'Our ETF weight', currentWeight: 'Current weight',
      portfolioWeight: 'In portfolio',
      holdingWeight: (weight: string) => `This ETF is ${weight}% of the current portfolio.`,
      loading: 'Loading sector weights', failed: 'Sector allocation could not be loaded.' };
  const mappedSectors = collapseEtfSectors(data?.sectors ?? []);
  const sourceTotal = mappedSectors.reduce((sum, row) => sum + row.weight_pct, 0);

  return (
    <PanelDialog onClose={onClose} labelledBy="etf-sector-allocation-title">
      <div className="h-full min-h-0 flex flex-col rounded-xl border border-neutral-800/40 bg-card p-4">
        <div className="shrink-0 flex items-start justify-between gap-3 border-b border-neutral-800/30 pb-3">
          <div className="min-w-0">
            <h4 id="etf-sector-allocation-title" className="text-sm font-semibold text-fg-strong">
              {labels.title}
            </h4>
            <p className="mt-0.5 truncate text-xs text-fg-muted" title={name}>{name}</p>
            <p className="mt-1 font-mono text-[11px] text-fg-faint">{isin}</p>
            <p className="mt-1 text-[11px] text-fg-faint">
              {labels.holdingWeight(portfolioWeightPct.toFixed(2))}
            </p>
            {data && (
              <a href={data.source_url} target="_blank" rel="noreferrer"
                className="mt-1 inline-flex text-[11px] text-accent-300 hover:text-accent-200 hover:underline">
                {labels.source}: {data.source}
              </a>
            )}
          </div>
          <button type="button" onClick={onClose}
            className="cursor-pointer rounded-lg border border-neutral-700 px-3 py-1.5 text-xs text-fg-muted hover:border-accent-500/50 hover:text-accent-300">
            {labels.close}
          </button>
        </div>

        <div className="min-h-0 flex-1 overflow-auto py-4">
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
                    title={row.sector}>
                    <span className="h-2.5 w-2.5 shrink-0 rounded-full"
                      style={{ background: colorForSector(row.sector) }} aria-hidden />
                    <span className="truncate">{row.sector}</span>
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
            </div>
          )}
        </div>

        {data && date && (
          <div className="shrink-0 border-t border-neutral-800/30 pt-3 text-[11px] text-fg-faint">
            <span>{labels.asOf} {date}</span>
          </div>
        )}
      </div>
    </PanelDialog>
  );
}
