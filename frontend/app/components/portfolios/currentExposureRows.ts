import type { EtfCurrencyAllocationResponse, EtfRegionAllocationResponse, EtfSectorAllocationResponse, ModelPortfolioAnalysis } from '../../../lib/types/api';
import { portfolioSectorBucket, PORTFOLIO_SECTORS, type SectorChartRow, type ClassificationEvidence } from './etfSectorLookThrough';

type Holding = NonNullable<ModelPortfolioAnalysis['book_holdings']>[number];

/** Both axes describe the same current equity positions on the whole-book denominator. */
export function currentExposureRows(
  base: SectorChartRow[], holdings: Holding[], axis: 'sector' | 'region' | 'currency',
  includeFunds: boolean, lookThrough: boolean,
  sectors: Readonly<Record<string, EtfSectorAllocationResponse>>,
  regions: Readonly<Record<string, EtfRegionAllocationResponse>>,
  currencies: Readonly<Record<string, EtfCurrencyAllocationResponse>> = {},
  unavailableReasons: Readonly<Record<string, string>> = {},
): SectorChartRow[] {
  const rows = new Map<string, SectorChartRow>(base.map(row => [row.bucket,
    { ...row, portfolio_pct: 0, diff_pct: -(row.benchmark_pct ?? 0), holdings: [] }]));
  const originalHoldings = base.flatMap(row => row.holdings ?? []);
  const add = (bucket: string | null | undefined, weight: number, holding: Holding, evidence?: Omit<ClassificationEvidence, 'portfolioWeightPct'>) => {
    if (weight <= 0) return;
    const name = bucket?.trim() || 'Unclassified';
    const normalized = axis === 'sector' ? portfolioSectorBucket(name) : name;
    const allowed: readonly string[] = axis === 'sector' ? PORTFOLIO_SECTORS
      : ['North America', 'Europe', 'Pacific', 'Emerging Markets'];
    const key = (axis === 'currency' ? /^[A-Z]{3}$/.test(normalized) : allowed.includes(normalized)) ? normalized : 'Unclassified';
    const row = rows.get(key) ?? { bucket: key, portfolio_pct: 0, benchmark_pct: 0, diff_pct: 0, holdings: [] };
    row.portfolio_pct = (row.portfolio_pct ?? 0) + weight;
    row.diff_pct = row.portfolio_pct - (row.benchmark_pct ?? 0);
    const original = holding.isin ? originalHoldings.find(item => item.isin?.trim().toUpperCase() === holding.isin!.trim().toUpperCase()) : undefined;
    (row.holdings ??= []).push({ ...original, name: holding.name, isin: holding.isin, weight_pct: weight,
      in_both: holding.in_both || original?.in_both || false,
      issuer_key: holding.issuer_key ?? original?.issuer_key,
      classification_evidence: key === 'Unclassified' ? { reason: `No ${axis} classification is available for this holding.`, ...evidence, portfolioWeightPct: weight } : undefined,
      asset_class: holding.bucket, classified_as: key, via_names: holding.via_names ?? [] });
    rows.set(key, row);
  };
  for (const holding of holdings) {
    if (holding.bucket !== 'Equity' || (!includeFunds && holding.is_fund)) continue;
    const weight = !includeFunds && !lookThrough && holding.sources?.length
      ? holding.sources.filter(source => source.label == null).reduce((sum, source) => sum + Math.max(0, source.weight_now_pct), 0)
      : holding.weight_now_pct ?? 0;
    if (!Number.isFinite(weight) || weight <= 0) continue;
    if (!holding.is_fund) {
      const previousRegions = axis === 'region' && holding.isin
        ? base.filter(row => row.holdings?.some(item => item.isin === holding.isin)) : [];
      add(axis === 'currency' ? ({ GBp: 'GBP', GBX: 'GBP', ZAc: 'ZAR', ILA: 'ILS', KWF: 'KWD' }[holding.currency ?? ''] ?? holding.currency) : axis === 'sector' ? holding.sector : holding.region
        ?? (previousRegions.length === 1 ? previousRegions[0].bucket : undefined), weight, holding);
      continue;
    }
    const isin = holding.isin?.trim().toUpperCase() ?? '';
    const allocation = axis === 'sector' ? sectors[isin] : axis === 'region' ? regions[isin] : currencies[isin];
    const evidenceSource = { source: allocation?.source, sourceUrl: allocation?.source_url };
    const breakdown = axis === 'sector'
      ? sectors[isin]?.sectors.map(row => ({ bucket: row.sector, weight: row.weight_pct, label: row.provider_sectors?.join(' + ') ?? row.sector, reason: 'The provider does not identify a sector for this portion.' }))
      : axis === 'currency' ? currencies[isin]?.holdings.map(row => ({ bucket: row.currency, weight: row.weight_pct, label: row.name, reason: row.reason ?? 'The provider does not identify the currency for this portion.' }))
      : regions[isin]?.regions.map(row => ({ bucket: row.region, weight: row.weight_pct, label: row.provider_weights?.map(item => item.country).join(' + ') ?? row.region, reason: 'The published country labels cannot be assigned to a region.' }));
    const total = breakdown?.reduce((sum, row) => sum + row.weight, 0) ?? 0;
    if (!breakdown?.length || !Number.isFinite(total) || total <= 0 || total > 100.01 + 1e-8
      || breakdown.some(row => !Number.isFinite(row.weight) || row.weight < 0)) {
      add('Unclassified', weight, holding, { ...evidenceSource,
        reason: breakdown?.length ? 'The fund breakdown contains invalid weights and cannot be used.'
          : (unavailableReasons[isin] ?? (holding.sector_allocation_available === false
            ? `No supported ${axis} allocation source is configured for this fund.`
            : `The ${axis} breakdown has not been loaded for this fund.`)) });
      continue;
    }
    // Correct publication rounding only. Genuine partial coverage leaves a visible remainder.
    const roundedFull = Math.abs(total - 100) <= 0.01 + 1e-8;
    const denominator = roundedFull ? total : 100;
    for (const row of breakdown) add(row.bucket, weight * row.weight / denominator, holding, {
      ...evidenceSource, reason: row.reason, providerLabel: row.label, fundWeightPct: row.weight,
    });
    if (!roundedFull) add('Unclassified', weight * (100 - total) / 100, holding, {
      ...evidenceSource, reason: 'The provider does not publish the remaining allocation.',
      providerLabel: 'Unreported remainder', fundWeightPct: 100 - total,
    });
  }
  // Allocate rounding hundredths so the sum of displayed rows equals the same holding total.
  const result = [...rows.values()];
  const target = Math.round(result.reduce((sum, row) => sum + (row.portfolio_pct ?? 0), 0) * 100);
  const ranked = result.map(row => ({ row, raw: (row.portfolio_pct ?? 0) * 100,
    units: Math.floor((row.portfolio_pct ?? 0) * 100) }));
  let remaining = target - ranked.reduce((sum, row) => sum + row.units, 0);
  ranked.sort((a, b) => (b.raw - b.units) - (a.raw - a.units) || a.row.bucket.localeCompare(b.row.bucket));
  for (const entry of ranked) {
    entry.row.portfolio_pct = (entry.units + (remaining-- > 0 ? 1 : 0)) / 100;
    entry.row.diff_pct = entry.row.portfolio_pct - (entry.row.benchmark_pct ?? 0);
  }
  return result;
}
