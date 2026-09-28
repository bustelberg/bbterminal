import type { EtfSectorAllocationResponse, ModelPortfolioAnalysis } from '../../../lib/types/api';

type Axis = NonNullable<ModelPortfolioAnalysis['axes']>[number];
export type SectorChartRow = Axis['rows'][number];
type BookHolding = NonNullable<ModelPortfolioAnalysis['book_holdings']>[number];
type EtfSector = EtfSectorAllocationResponse['sectors'][number];

// The existing portfolio/benchmark chart uses Yahoo's sector vocabulary. ETF providers commonly
// publish GICS display names instead. Resolve both onto the buckets already established in that
// chart before doing any arithmetic, or "Healthcare" and "Health Care" become two investments.
const PORTFOLIO_SECTOR_ALIASES: Readonly<Record<string, string>> = {
  'information technology': 'Technology',
  technology: 'Technology',
  tech: 'Technology',
  'health care': 'Healthcare',
  healthcare: 'Healthcare',
  'consumer discretionary': 'Consumer Cyclical',
  'consumer cyclical': 'Consumer Cyclical',
  'consumer cyclicals': 'Consumer Cyclical',
  'consumer services': 'Consumer Cyclical',
  'consumer staples': 'Consumer Defensive',
  'consumer defensive': 'Consumer Defensive',
  'consumer non-cyclical': 'Consumer Defensive',
  'consumer non-cyclicals': 'Consumer Defensive',
  finance: 'Financials',
  financial: 'Financials',
  financials: 'Financials',
  'financial services': 'Financials',
  'basic materials': 'Materials',
  materials: 'Materials',
  communication: 'Communication Services',
  communications: 'Communication Services',
  'communication services': 'Communication Services',
  'business services': 'Industrials',
  industrials: 'Industrials',
  energy: 'Energy',
  utilities: 'Utilities',
  'real estate': 'Real Estate',
  cash: 'Unclassified',
  'cash and/or derivatives': 'Unclassified',
  other: 'Unclassified',
  'non-corporate': 'Unclassified',
  unclassified: 'Unclassified',
};

export function portfolioSectorBucket(sector: string): string {
  const normalized = ' '.concat(sector).trim().toLowerCase().replace(/\s+/g, ' ');
  return PORTFOLIO_SECTOR_ALIASES[normalized] ?? sector.trim();
}

/** Combine every provider/GICS spelling that belongs to one of the chart's existing sectors. */
export function collapseEtfSectors(sectors: EtfSector[]): EtfSector[] {
  const collapsed = new Map<string, EtfSector>();
  for (const row of sectors) {
    const sector = portfolioSectorBucket(row.sector);
    const current = collapsed.get(sector);
    if (!current) {
      collapsed.set(sector, { ...row, sector, provider_sectors: [...row.provider_sectors],
        provider_weights: row.provider_weights.map((provider) => ({ ...provider })) });
      continue;
    }
    current.weight_pct += row.weight_pct;
    for (const provider of row.provider_weights) {
      const existing = current.provider_weights.find((item) => item.sector === provider.sector);
      if (existing) existing.weight_pct += provider.weight_pct;
      else current.provider_weights.push({ ...provider });
    }
    current.provider_sectors = current.provider_weights.map((provider) => provider.sector);
  }
  return [...collapsed.values()];
}

export function normalizedEtfSectorWeight(weightPct: number, allocationTotal: number): number {
  return allocationTotal > 0 ? weightPct / allocationTotal * 100 : 0;
}

export function etfSectorPortfolioContribution(
  portfolioWeightPct: number, sectorWeightPct: number, allocationTotal: number,
): number {
  return portfolioWeightPct * normalizedEtfSectorWeight(sectorWeightPct, allocationTotal) / 100;
}

/**
 * Fold verified ETF look-through into the existing current-book sector rows.
 *
 * Both inputs use percentages of the complete current AIRS book. An ETF at 12% whose source
 * breakdown says 25% Financials therefore contributes 3 percentage points to Financials. Source
 * tables occasionally total 99.99 or 100.01 after publication rounding, so divide by their
 * reported total: the sectors added for one ETF then sum to exactly the weight actually owned.
 */
export function addEtfSectorWeights(
  rows: SectorChartRow[],
  holdings: BookHolding[],
  allocations: Readonly<Record<string, EtfSectorAllocationResponse>>,
): SectorChartRow[] {
  const bySector = new Map<string, SectorChartRow>();
  for (const row of rows) {
    const bucket = portfolioSectorBucket(row.bucket);
    const current = bySector.get(bucket);
    if (!current) {
      bySector.set(bucket, { ...row, bucket });
      continue;
    }
    current.portfolio_pct = (current.portfolio_pct ?? 0) + (row.portfolio_pct ?? 0);
    current.benchmark_pct = (current.benchmark_pct ?? 0) + (row.benchmark_pct ?? 0);
    current.diff_pct = (current.portfolio_pct ?? 0) - (current.benchmark_pct ?? 0);
    current.holdings = [...(current.holdings ?? []), ...(row.holdings ?? [])];
  }

  for (const holding of holdings) {
    const isin = holding.isin?.trim().toUpperCase();
    const holdingWeight = holding.weight_now_pct ?? 0;
    if (!holding.is_fund || !holding.sector_allocation_available || !isin || holdingWeight <= 0) {
      continue;
    }
    const allocation = allocations[isin];
    const sectors = collapseEtfSectors(allocation?.sectors ?? []);
    const allocationTotal = sectors.reduce(
      (sum, sector) => sum + sector.weight_pct, 0,
    );
    if (!allocation || allocationTotal <= 0) continue;

    for (const sector of sectors) {
      const contribution = etfSectorPortfolioContribution(
        holdingWeight, sector.weight_pct, allocationTotal,
      );
      const current = bySector.get(sector.sector);
      if (current) {
        const portfolio = (current.portfolio_pct ?? 0) + contribution;
        current.portfolio_pct = portfolio;
        current.diff_pct = portfolio - (current.benchmark_pct ?? 0);
      } else {
        bySector.set(sector.sector, {
          bucket: sector.sector,
          portfolio_pct: contribution,
          benchmark_pct: 0,
          diff_pct: contribution,
          holdings: [],
        });
      }
    }
  }

  return [...bySector.values()];
}
