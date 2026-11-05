export type Holding = { isin: string | null; name: string | null; weight: string | null };
export type HoldingRule = { leonteq_key: string; airs_key: string | null };
// Reserved rule namespace for an explicitly confirmed AIRS-only sold position.
export const soldRuleKey = (airsKey: string) => `sold:${airsKey}`;
const isinKey = (isin: string | null) => {
  const value = isin?.trim().toUpperCase() ?? '';
  return /^[A-Z]{2}[A-Z0-9]{9}\d$/.test(value) ? value : null;
};

export const holdingKey = (row: Holding) => {
  const isin = isinKey(row.isin);
  return isin ? `isin:${isin}` : `name:${(row.name ?? '').trim().normalize('NFC').toLocaleLowerCase()}`;
};

/** Manual choices take precedence; original rows and weights stay intact. */
export function matchHoldings(leonteq: Holding[], airs: Holding[], rules: HoldingRule[] = []) {
  const group = (rows: Holding[]) => {
    const result = new Map<string, Holding[]>();
    for (const row of rows) {
      const key = holdingKey(row);
      result.set(key, [...(result.get(key) ?? []), row]);
    }
    return result;
  };
  const left = group(leonteq), right = group(airs);
  const matched: { isin: string; leonteqKey: string; airsKey: string; manual: boolean; leonteq: Holding[]; airs: Holding[] }[] = [];
  const usedLeft = new Set<string>(), usedRight = new Set<string>();
  const sold: { airsKey: string; rows: Holding[]; conflict: boolean }[] = [];
  for (const rule of rules) {
    if (rule.airs_key?.startsWith('isin:') && rule.leonteq_key === soldRuleKey(rule.airs_key) && right.has(rule.airs_key)) {
      sold.push({ airsKey: rule.airs_key, rows: right.get(rule.airs_key)!, conflict: left.has(rule.airs_key) });
      usedRight.add(rule.airs_key);
    }
  }
  const overridden = new Set(rules.map((rule) => rule.leonteq_key));
  function add(leftKey: string, rightKey: string, manual: boolean) {
    const rows = left.get(leftKey), other = right.get(rightKey);
    if (!rows || !other || usedLeft.has(leftKey) || usedRight.has(rightKey)) return;
    const leftIsin = isinKey(rows[0].isin), rightIsin = isinKey(other[0].isin);
    matched.push({ isin: leftIsin && leftIsin === rightIsin ? leftIsin : `${leftIsin ?? '—'} ↔ ${rightIsin ?? '—'}`,
      leonteqKey: leftKey, airsKey: rightKey, manual, leonteq: rows, airs: other });
    usedLeft.add(leftKey); usedRight.add(rightKey);
  }
  for (const rule of rules) if (rule.airs_key) add(rule.leonteq_key, rule.airs_key, true);
  for (const key of left.keys()) if (key.startsWith('isin:') && !overridden.has(key)) add(key, key, false);
  return {
    matched,
    sold,
    leonteqOnly: leonteq.filter((row) => !usedLeft.has(holdingKey(row))),
    airsOnly: airs.filter((row) => !usedRight.has(holdingKey(row))),
  };
}
