import type { ModelPortfolioAnalysis } from '../../../lib/types/api';

type Holding = NonNullable<ModelPortfolioAnalysis['book_holdings']>[number];
export type Exposure = { name: string; weight: number; routes: string[] };

/** Complete-book AIRS weights: never renormalise an incompletely classified sleeve to 100%. */
export function airsRiskExposures(holdings: Holding[], bands: ModelPortfolioAnalysis['bands'] = []) {
  const companies = new Map<string, Exposure>();
  const classes = new Map<string, number>();
  const sectors = new Map<string, number>();
  const currencies = new Map<string, number>();
  const wrappers = new Map<string, number>();
  let total = 0;
  let companyCoverage = 0;
  const add = (map: Map<string, number>, key: string, value: number) => map.set(key, (map.get(key) ?? 0) + value);
  for (const holding of holdings) {
    const weight = holding.weight_now_pct;
    if (weight == null || !Number.isFinite(weight) || weight <= 0) continue;
    total += weight;
    add(classes, holding.bucket || 'Unknown', weight);
    add(currencies, holding.currency || 'Unknown', weight);
    for (const source of holding.sources ?? []) {
      if (source.label && Number.isFinite(source.weight_now_pct) && source.weight_now_pct > 0) {
        add(wrappers, source.label, source.weight_now_pct);
      }
    }
    if (holding.bucket !== 'Equity' || holding.is_fund) continue;
    add(sectors, holding.sector || 'Unknown', weight);
    if (!holding.isin && holding.company_id == null) continue;
    const key = holding.company_id != null ? `company:${holding.company_id}` : `isin:${holding.isin}`;
    const company = companies.get(key) ?? { name: holding.name || holding.isin || 'Unknown', weight: 0, routes: [] };
    company.weight += weight;
    company.routes = [...new Set([...company.routes, ...(holding.via_names ?? [])])];
    companies.set(key, company);
    companyCoverage += weight;
  }
  const sorted = (map: Map<string, number>): Exposure[] => [...map].map(([name, weight]) => ({ name, weight, routes: [] })).sort((a, b) => b.weight - a.weight);
  const companyRows = [...companies.values()].sort((a, b) => b.weight - a.weight);
  const complete = holdings.length > 0 && Math.abs(total - 100) <= .5;
  const breaches = (complete ? bands ?? [] : []).flatMap<{ name: string; actual: number; limit: number; direction: 'below' | 'above' }>((band) => {
    const actual = classes.get(band.bucket) ?? 0;
    if (band.min_pct != null && actual < band.min_pct) return [{ name: band.bucket, actual, limit: band.min_pct, direction: 'below' as const }];
    if (band.max_pct != null && actual > band.max_pct) return [{ name: band.bucket, actual, limit: band.max_pct, direction: 'above' as const }];
    return [];
  });
  return { classes: sorted(classes), sectors: sorted(sectors), currencies: sorted(currencies), wrappers: sorted(wrappers),
    companies: companyRows, companyCoverage, total, complete, breaches,
    topFive: companyRows.slice(0, 5).reduce((sum, row) => sum + row.weight, 0),
    repeated: companyRows.filter((row) => row.routes.length > 1) };
}
