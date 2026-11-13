import type { SectorChartRow, ClassificationEvidence } from './etfSectorLookThrough';

export function sectorHoldingsCompanies(row: SectorChartRow) {
  const sides = [['portfolio', row.holdings ?? []], ['benchmark', row.benchmark_holdings ?? []]] as const;
  const parent = new Map<string, string>();
  const root = (key: string): string => {
    const next = parent.get(key);
    if (!next) { parent.set(key, key); return key; }
    if (next === key) return key;
    const result = root(next);
    parent.set(key, result);
    return result;
  };
  for (const [, entries] of sides) {
    for (const holding of entries) {
      const isin = holding.isin?.trim().toUpperCase();
      const issuer = holding.issuer_key;
      if (isin && issuer) {
        // A shared ISIN is a stronger bridge than differently spelled issuer labels.
        const listingRoot = root(`isin:${isin}`), issuerRoot = root(`issuer:${issuer}`);
        if (listingRoot !== issuerRoot) parent.set(listingRoot, issuerRoot);
      }
    }
  }
  const companies = new Map<string, {
    name: string; via: Set<string>; portfolio?: number; benchmark?: number; inBoth: boolean; evidence: { portfolio: ClassificationEvidence[]; benchmark: ClassificationEvidence[] };
  }>();
  for (const [side, entries] of sides) {
    entries.forEach((holding, index) => {
      // The backend provides the snapshot's issuer identity. Display names may
      // differ between AIRS, Yahoo and iShares, and are never join keys here.
      const isin = holding.isin?.trim().toUpperCase();
      const key = root(holding.issuer_key ? `issuer:${holding.issuer_key}`
        : isin ? `isin:${isin}` : `${side}-${index}`);
      const company = companies.get(key) ?? { name: holding.name || '—', via: new Set<string>(), inBoth: false, evidence: { portfolio: [], benchmark: [] } };
      if (holding.classification_evidence) company.evidence[side].push(holding.classification_evidence);
      company.inBoth ||= holding.in_both ?? false;
      for (const name of holding.via_names ?? []) company.via.add(name);
      company[side] = (company[side] ?? 0) + (holding.weight_pct ?? 0);
      companies.set(key, company);
    });
  }
  return [...companies.entries()];
}
