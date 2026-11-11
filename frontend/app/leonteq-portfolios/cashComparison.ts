import type { CompositionResult } from './CompositionTable';
import type { AirsComposition } from './AirsCompositionView';
import { isCash } from './matchHoldings';
import { weight } from './rebalancePlan';

function sum(values: (number | null)[]) {
  return values.length && values.every(value => value != null && Number.isFinite(value))
    ? values.reduce<number>((total, value) => total + value!, 0) : null;
}

export function cashComparison(leonteq: CompositionResult | undefined, airs: AirsComposition) {
  const source = leonteq?.success ? sum((leonteq.groups ?? []).flatMap(group => {
    const cashGroup = isCash({ name: group.name, isin: null, weight: null });
    if (cashGroup && !group.positions.length) return [weight(group.weight)];
    return group.positions.filter(row => cashGroup || isCash(row)).map(row => weight(row.weight));
  })) : null;
  const target = sum(airs.rows.filter(row => isCash({ name: row.Fonds, isin: row.ISINCode, weight: null })).map(row => row.Percentage));
  return { leonteq: source, airs: target, difference: source != null && target != null ? target - source : null };
}
