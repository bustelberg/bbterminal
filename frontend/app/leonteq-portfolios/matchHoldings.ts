export type Holding = { isin: string | null; name: string | null; weight: string | null };
export type HoldingRule = { leonteq_key: string; airs_key: string | null };
const cashNames = new Set(['cash', 'eur', 'euro', 'usd', 'us dollar', 'us dollars', 'gbp', 'british pound', 'chf', 'swiss franc', 'liquiditeiten', 'liquide middelen']);
export const isCash = (row: Holding) => {
  const name = (row.name ?? '').trim().toLowerCase();
  return name === 'effectenrekening' || (!isinKey(row.isin) && cashNames.has(name));
};

function cashTotal(rows: Holding[]): Holding {
  const values = rows.map(row => (row.weight ?? '').trim().replace(/%$/, '').trim().replace(',', '.'));
  const weights = values.map(Number);
  const valid = values.every((value, i) => /^[+-]?\d+(?:\.\d+)?$/.test(value) && Number.isFinite(weights[i]));
  return { isin: null, name: `Cash (${rows.map(row => row.name).join(' + ')})`,
    weight: valid ? `${Number(weights.reduce((sum, value) => sum + value, 0).toFixed(6))}%` : null };
}
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

// For portfolio exposure matching only; never use this identity for price/quantity conversion.
function companyName(name: string | null) {
  if (!name || /\b(etf|ucits|fund|bond|note|preferred|pref|class)\b|\d|%/i.test(name)) return '';
  const text = name.toLowerCase().normalize('NFKD').replace(/[\u0300-\u036f]/g, '')
    .replace(/\breg\s*[- ]?s\b/g, '').replace(/\bw\s*\/\s*i\b/g, '');
  const ignored = new Set(['and', 'corp', 'corporation', 'inc', 'incorporated', 'ltd', 'limited', 'plc', 'sa', 'se', 'ag', 'nv', 'adr', 'gdr', 'sponsored', 'spons', 'eur', 'usd', 'inr', 'jpy', 'krw']);
  return text.replace(/[^a-z0-9 ]/g, ' ').split(/\s+/).filter(token => token && !ignored.has(token))
    .map(token => token === 'hynic' ? 'hynix' : ['electr', 'electron'].includes(token) ? 'electronics' : token).sort().join(' ');
}

/** Manual choices take precedence; original rows and weights stay intact. */
export function matchHoldings(leonteq: Holding[], airs: Holding[], rules: HoldingRule[] = []) {
  const leftCash = leonteq.filter(isCash), rightCash = airs.filter(isCash);
  const cashMatched = leftCash.length > 0 && rightCash.length > 0;
  if (cashMatched) {
    leonteq = leonteq.filter(row => !isCash(row));
    airs = airs.filter(row => !isCash(row));
  }
  const group = (rows: Holding[]) => {
    const result = new Map<string, Holding[]>();
    for (const row of rows) {
      const key = holdingKey(row);
      result.set(key, [...(result.get(key) ?? []), row]);
    }
    return result;
  };
  const left = group(leonteq), right = group(airs);
  const matched: { isin: string; leonteqKey: string; airsKey: string; manual: boolean; cash?: boolean; company?: boolean; leonteq: Holding[]; airs: Holding[] }[] = [];
  if (cashMatched) matched.push({ isin: 'Cash', leonteqKey: 'cash:total', airsKey: 'cash:total', manual: false, cash: true,
    leonteq: [cashTotal(leftCash)], airs: [cashTotal(rightCash)] });
  const usedLeft = new Set<string>(), usedRight = new Set<string>();
  const sold: { airsKey: string; rows: Holding[]; conflict: boolean }[] = [];
  for (const rule of rules) {
    if (rule.airs_key?.startsWith('isin:') && rule.leonteq_key === soldRuleKey(rule.airs_key) && right.has(rule.airs_key)) {
      const soldRows = right.get(rule.airs_key)!;
      const name = companyName(soldRows[0].name);
      sold.push({ airsKey: rule.airs_key, rows: soldRows,
        conflict: left.has(rule.airs_key) || Boolean(name && leonteq.some(row => companyName(row.name) === name)) });
      usedRight.add(rule.airs_key);
    }
  }
  const overridden = new Set(rules.map((rule) => rule.leonteq_key));
  function add(leftKey: string, rightKey: string, manual: boolean, company = false) {
    const rows = left.get(leftKey), other = right.get(rightKey);
    if (!rows || !other || usedLeft.has(leftKey) || usedRight.has(rightKey)) return;
    const leftIsin = isinKey(rows[0].isin), rightIsin = isinKey(other[0].isin);
    matched.push({ isin: leftIsin && leftIsin === rightIsin ? leftIsin : `${leftIsin ?? '—'} ↔ ${rightIsin ?? '—'}`,
      leonteqKey: leftKey, airsKey: rightKey, manual, company, leonteq: rows, airs: other });
    usedLeft.add(leftKey); usedRight.add(rightKey);
  }
  for (const rule of rules) if (rule.airs_key) add(rule.leonteq_key, rule.airs_key, true);
  for (const key of left.keys()) if (key.startsWith('isin:') && !overridden.has(key)) add(key, key, false);
  const reservedRight = new Set(rules.map(rule => rule.airs_key).filter(Boolean));
  const availableLeft = [...left].filter(([key]) => key.startsWith('isin:') && !usedLeft.has(key) && !overridden.has(key));
  const availableRight = [...right].filter(([key]) => key.startsWith('isin:') && !usedRight.has(key) && !reservedRight.has(key));
  for (const [leftKey, rows] of availableLeft) {
    const name = companyName(rows[0].name);
    if (!name || !rows.every(row => companyName(row.name) === name)) continue;
    const candidates = availableRight.filter(([, others]) => others.every(row => companyName(row.name) === name));
    const peers = availableLeft.filter(([, others]) => others.some(row => companyName(row.name) === name));
    if (candidates.length === 1 && peers.length === 1) add(leftKey, candidates[0][0], false, true);
  }
  return {
    matched,
    sold,
    leonteqOnly: leonteq.filter((row) => !usedLeft.has(holdingKey(row))),
    airsOnly: airs.filter((row) => !usedRight.has(holdingKey(row))),
  };
}
