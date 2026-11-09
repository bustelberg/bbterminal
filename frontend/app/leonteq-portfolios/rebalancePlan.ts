import { matchHoldings, type Holding, type HoldingRule } from './matchHoldings';

export type RebalanceAction = { id: string; name: string; isin: string; current: number; target: number };
export function weight(value: string | null): number | null {
  if (value == null) return null;
  const text = value.trim().replace(/%$/, '').trim().replace(',', '.');
  if (!/^\d+(?:\.\d+)?$/.test(text)) return null;
  const number = Number(text);
  return Number.isFinite(number) && number >= 0 && number <= 100 ? number : null;
}
export function rebalancePlan(left: Holding[], right: Holding[], rules: HoldingRule[]) {
  const matches = matchHoldings(left, right, rules);
  const issues: string[] = [];
  const actions: RebalanceAction[] = [];
  if (matches.leonteqOnly.length || matches.airsOnly.length) issues.push('Unmatched holdings: connect all rows in Connections first.');
  for (const pair of matches.matched) {
    if (pair.cash) continue; // Aggregate cash is a comparison, not a tradeable instrument.
    if (pair.leonteq.length !== 1 || pair.airs.length !== 1) { issues.push(`Ambiguous match: ${pair.isin}`); continue; }
    const current = weight(pair.airs[0].weight), target = weight(pair.leonteq[0].weight);
    if (current === null || target === null) { issues.push(`Invalid weight: ${pair.isin}`); continue; }
    if (Math.abs(current - target) > 0.0000005) actions.push({ id: pair.airsKey, name: pair.airs[0].name ?? '—', isin: pair.isin, current, target });
  }
  for (const sold of matches.sold) {
    if (sold.conflict) { issues.push(`Marked sold but present in Leonteq: ${sold.airsKey}`); continue; }
    if (sold.rows.length !== 1) { issues.push(`Ambiguous sold holding: ${sold.airsKey}`); continue; }
    const row = sold.rows[0], current = weight(row.weight);
    if (current === null) { issues.push(`Invalid weight: ${sold.airsKey}`); continue; }
    if (current > 0) actions.push({ id: sold.airsKey, name: row.name ?? '—', isin: row.isin ?? '—', current, target: 0 });
  }
  if (!left.length || !right.length) issues.push('Composition is empty.');
  return { actions, issues };
}
