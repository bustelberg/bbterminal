import type { CompositionResult } from './CompositionTable';
import type { AirsComposition } from './AirsCompositionView';
import { holdingKey, matchHoldings, type HoldingRule } from './matchHoldings';
import { weight } from './rebalancePlan';

export type RecentTrade = { id: string; date: string; event: 'Buy' | 'Sell'; name: string; isin: string | null; quantity: number; price: number; currency: string | null };
export type TransactionBundle = {
  success: boolean; transactions: RecentTrade[]; from: string; to: string; retrieved_at: string;
  leonteq: CompositionResult; airs: AirsComposition; rules: HoldingRule[];
};
export type RecentWeightRow = {
  operation: 'add' | 'update';
  key: string; name: string; isin: string | null; trades: RecentTrade[]; latest: string;
  airsName: string | null; airsIsin: string | null; rowId: number | null;
  current: number | null; target: number | null; reason: string | null; aligned: boolean;
};

export function recentWeightPlan(bundle: TransactionBundle): RecentWeightRow[] {
  const left = bundle.leonteq.groups?.flatMap(group => group.positions) ?? [];
  const right = bundle.airs.rows.map(row => ({ name: row.Fonds, isin: row.ISINCode, weight: row.Percentage == null ? null : String(row.Percentage) }));
  const matches = matchHoldings(left, right, bundle.rules);
  const groups = new Map<string, RecentTrade[]>();
  for (const trade of bundle.transactions) {
    const key = trade.isin?.trim().toUpperCase() || `missing:${trade.name}`;
    groups.set(key, [...(groups.get(key) ?? []), trade]);
  }
  return [...groups].map(([key, trades]) => {
    const first = trades[0], latest = trades.map(trade => trade.date).sort().at(-1)!;
    const sourceKey = first.isin ? `isin:${first.isin.toUpperCase()}` : null;
    const pair = matches.matched.find(pair => pair.leonteqKey === sourceKey && !pair.cash);
    let airsKey = pair?.airsKey;
    let target = pair?.leonteq.length === 1 ? weight(pair.leonteq[0].weight) : null;
    let reason: string | null = null;
    let operation: 'add' | 'update' = 'update';
    if (!sourceKey) reason = 'Needs connection: the trade has no ISIN.';
    else if (!pair && !left.some(row => holdingKey(row) === sourceKey)) {
      const rule = bundle.rules.find(rule => rule.leonteq_key === sourceKey);
      const candidates = rule?.airs_key === null ? [] : bundle.airs.rows.filter(row => holdingKey({ isin: row.ISINCode, name: row.Fonds, weight: null }) === (rule?.airs_key ?? sourceKey));
      const dates = left.map(row => row.price_date).filter((date): date is string => typeof date === 'string');
      const asOf = dates.length === left.length ? dates.sort()[0] : null;
      const latestTrades = trades.filter(trade => trade.date === latest);
      if (candidates.length === 1 && asOf && asOf >= latest && latestTrades.every(trade => trade.event === 'Sell')) {
        airsKey = holdingKey({ isin: candidates[0].ISINCode, name: candidates[0].Fonds, weight: null });
        if (matches.matched.some(pair => pair.airsKey === airsKey)) reason = 'Needs connection: this AIRS holding is connected to another instrument.';
        else target = 0;
      } else reason = 'Review required: a full sale needs a complete, dated Leonteq composition and one AIRS Fixed holding.';
    } else if (!pair) {
      const source = left.filter(row => holdingKey(row) === sourceKey);
      const hasRule = bundle.rules.some(rule => rule.leonteq_key === sourceKey);
      const exists = right.some(row => holdingKey(row) === sourceKey);
      if (source.length === 1 && !hasRule && !exists) {
        operation = 'add';
        target = weight(source[0].weight);
        if (target != null && target <= 0) reason = 'The Leonteq weight is zero. No new AIRS position is needed.';
      } else reason = 'Needs connection: connect this holding in Leonteq to AIRS Fixed.';
    }
    else if (pair.leonteq.length !== 1 || pair.airs.length !== 1) reason = 'Ambiguous holding connection. Review Connections first.';
    const candidates = bundle.airs.rows.filter(row => holdingKey({ isin: row.ISINCode, name: row.Fonds, weight: null }) === airsKey);
    const airs = candidates.length === 1 ? candidates[0] : null;
    const current = operation === 'add' ? 0 : airs?.Percentage ?? null;
    if (!reason && (target == null || current == null || !Number.isFinite(current) || current < 0 || current > 100)) reason = 'A source weight is missing or invalid.';
    if (!reason && ((operation !== 'add' && !airs?.id) || !bundle.airs.datum || bundle.airs.cache_upgrade_available)) reason = 'AIRS edit metadata is unavailable. Fetch transactions again.';
    if (!reason && target != null && Number(target.toFixed(6)) !== target) reason = 'The target exceeds AIRS’s six-decimal precision.';
    return { operation, key, name: first.name, isin: first.isin, trades, latest, airsName: airs?.Fonds ?? null,
      airsIsin: operation === 'add' ? first.isin : airs?.ISINCode ?? null, rowId: airs?.id ?? null, current, target, reason,
      aligned: !reason && current != null && target != null && Math.abs(current - target) <= 0.0000005 };
  }).sort((a, b) => b.latest.localeCompare(a.latest) || a.name.localeCompare(b.name));
}
