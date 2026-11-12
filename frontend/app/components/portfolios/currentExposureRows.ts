import type { EtfRegionAllocationResponse, EtfSectorAllocationResponse, ModelPortfolioAnalysis } from '../../../lib/types/api';
import { portfolioSectorBucket, PORTFOLIO_SECTORS, type SectorChartRow } from './etfSectorLookThrough';

type Holding = NonNullable<ModelPortfolioAnalysis['book_holdings']>[number];

/** Both axes describe the same current equity positions on the whole-book denominator. */
export function currentExposureRows(
  base: SectorChartRow[], holdings: Holding[], axis: 'sector' | 'region',
  includeFunds: boolean, lookThrough: boolean,
  sectors: Readonly<Record<string, EtfSectorAllocationResponse>>,
  regions: Readonly<Record<string, EtfRegionAllocationResponse>>,
): SectorChartRow[] {
  const rows = new Map<string, SectorChartRow>(base.map(row => [row.bucket,
    { ...row, portfolio_pct: 0, diff_pct: -(row.benchmark_pct ?? 0), holdings: [] }]));
  const add = (bucket: string | null | undefined, weight: number, holding: Holding) => {
    const name = bucket?.trim() || 'Unclassified';
    const normalized = axis === 'sector' ? portfolioSectorBucket(name) : name;
    const allowed: readonly string[] = axis === 'sector' ? PORTFOLIO_SECTORS
      : ['North America', 'Europe', 'Pacific', 'Emerging Markets'];
    const key = allowed.includes(normalized) ? normalized : 'Unclassified';
    const row = rows.get(key) ?? { bucket: key, portfolio_pct: 0, benchmark_pct: 0, diff_pct: 0, holdings: [] };
    row.portfolio_pct = (row.portfolio_pct ?? 0) + weight;
    row.diff_pct = row.portfolio_pct - (row.benchmark_pct ?? 0);
    (row.holdings ??= []).push({ name: holding.name, isin: holding.isin, weight_pct: weight,
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
      add(axis === 'sector' ? holding.sector : holding.region
        ?? (previousRegions.length === 1 ? previousRegions[0].bucket : undefined), weight, holding);
      continue;
    }
    const isin = holding.isin?.trim().toUpperCase() ?? '';
    const breakdown = axis === 'sector'
      ? sectors[isin]?.sectors.map(row => ({ bucket: row.sector, weight: row.weight_pct }))
      : regions[isin]?.regions.map(row => ({ bucket: row.region, weight: row.weight_pct }));
    const total = breakdown?.reduce((sum, row) => sum + row.weight, 0) ?? 0;
    if (!breakdown?.length || !Number.isFinite(total) || total <= 0 || total > 100.01 + 1e-8
      || breakdown.some(row => !Number.isFinite(row.weight) || row.weight < 0)) {
      add('Unclassified', weight, holding);
      continue;
    }
    // Correct publication rounding only. Genuine partial coverage leaves a visible remainder.
    const roundedFull = Math.abs(total - 100) <= 0.01 + 1e-8;
    const denominator = roundedFull ? total : 100;
    for (const row of breakdown) add(row.bucket, weight * row.weight / denominator, holding);
    if (!roundedFull) add('Unclassified', weight * (100 - total) / 100, holding);
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
