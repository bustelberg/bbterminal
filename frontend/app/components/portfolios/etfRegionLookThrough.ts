import type { EtfRegionAllocationResponse, EtfSectorAllocationResponse, ModelPortfolioAnalysis } from '../../../lib/types/api';
import { addEtfSectorWeights, type SectorChartRow } from './etfSectorLookThrough';

type Holding = NonNullable<ModelPortfolioAnalysis['book_holdings']>[number];

/** Regional axes also exclude fund wrappers. Reuse the same whole-book weighting arithmetic
 * as Sector, retaining Unclassified and the original country weights. */
export function addEtfRegionWeights(rows: SectorChartRow[], holdings: Holding[], allocations: Readonly<Record<string, EtfRegionAllocationResponse>>) {
  const mapped: Record<string, EtfSectorAllocationResponse> = {};
  for (const [isin, allocation] of Object.entries(allocations)) {
    mapped[isin] = { ...allocation, sectors: allocation.regions.map(row => ({
      sector: row.region, weight_pct: row.weight_pct,
      provider_sectors: row.provider_weights.map(item => item.country),
      provider_weights: row.provider_weights.map(item => ({ sector: item.country, weight_pct: item.weight_pct })),
    })) };
  }
  return addEtfSectorWeights(rows, holdings, mapped);
}
