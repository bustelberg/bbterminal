import { expect, it } from 'vitest';
import { matchHoldings, type Holding } from './matchHoldings';
import { rebalancePlan } from './rebalancePlan';

const row = (isin: string | null, name: string, weight = '5%'): Holding => ({ isin, name, weight });
const apple = 'US0378331005', microsoft = 'US5949181045', nvidia = 'US67066G1040';

it('marks an absent AIRS position sold and proposes zero without changing the original weight', () => {
  const left = [row(apple, 'Apple')], right = [row(apple, 'Apple'), row(microsoft, 'Microsoft', '1.35%')];
  const rules = [{ leonteq_key: `sold:isin:${microsoft}`, airs_key: `isin:${microsoft}` }];
  expect(matchHoldings(left, right, rules).airsOnly).toHaveLength(0);
  expect(rebalancePlan(left, right, rules)).toEqual({ issues: [], actions: [{ id: `isin:${microsoft}`, name: 'Microsoft', isin: microsoft, current: 1.35, target: 0 }] });
  expect(right[1].weight).toBe('1.35%');
  expect(matchHoldings(left, right, [{ ...rules[0], airs_key: null }]).airsOnly).toHaveLength(1);
});

it('blocks a stale sold marker when the position reappears or has duplicate AIRS rows', () => {
  const rules = [{ leonteq_key: `sold:isin:${microsoft}`, airs_key: `isin:${microsoft}` }];
  const reappeared = rebalancePlan([row(microsoft, 'Microsoft')], [row(microsoft, 'Microsoft')], rules);
  expect(reappeared.issues.join(' ')).toContain('present in Leonteq');
  expect(reappeared.actions).toHaveLength(0);
  const duplicate = rebalancePlan([row(apple, 'Apple')], [row(apple, 'Apple'), row(microsoft, 'Microsoft'), row(microsoft, 'Microsoft')], rules);
  expect(duplicate.issues.join(' ')).toContain('Ambiguous sold holding');
  expect(duplicate.actions).toHaveLength(0);
});

it('joins by normalized ISIN despite different source names and keeps weights separate', () => {
  const result = matchHoldings([row(` ${apple.toLowerCase()} `, 'APPLE INC', '4.9%')], [row(apple, 'Apple', '4.95%')]);
  expect(result.matched).toHaveLength(1);
  expect(result.matched[0].isin).toBe(apple);
  expect(result.matched[0].leonteq[0].weight).toBe('4.9%');
  expect(result.matched[0].airs[0].weight).toBe('4.95%');
});

it('remembers a disconnected ISIN instead of automatically reconnecting it', () => {
  const result = matchHoldings([row(apple, 'APPLE')], [row(apple, 'Apple')], [{ leonteq_key: `isin:${apple}`, airs_key: null }]);
  expect(result.matched).toEqual([]);
  expect(result.leonteqOnly).toHaveLength(1);
  expect(result.airsOnly).toHaveLength(1);
});

it('lets manual security connections override ISIN matches while automatically grouping cash', () => {
  const result = matchHoldings([row(apple, 'APPLE'), row(microsoft, 'MSFT'), row(null, 'EURO')],
    [row(microsoft, 'Microsoft'), row(null, 'Liquiditeiten')], [
      { leonteq_key: `isin:${apple}`, airs_key: `isin:${microsoft}` },
      { leonteq_key: 'name:euro', airs_key: 'name:liquiditeiten' },
    ]);
  expect(result.matched).toHaveLength(2);
  expect(result.matched.find(match => !match.cash)?.manual).toBe(true);
  expect(result.matched.find(match => match.cash)?.manual).toBe(false);
  expect(result.leonteqOnly.map((holding) => holding.name)).toEqual(['MSFT']);
  expect(result.airsOnly).toEqual([]);
});

it('does not silently rematch a manual selection when its target disappears', () => {
  const result = matchHoldings([row(apple, 'Apple')], [row(apple, 'Apple')], [{ leonteq_key: `isin:${apple}`, airs_key: `isin:${nvidia}` }]);
  expect(result.matched).toEqual([]);
});

it('keeps different ISINs in leftovers while matching recognised cash', () => {
  const left = [row(apple, 'Apple'), row(nvidia, 'NVIDIA'), row(null, 'EURO')];
  const right = [row(apple, 'Apple'), row(microsoft, 'Microsoft'), row(null, 'Liquiditeiten')];
  const result = matchHoldings(left, right);
  expect(result.matched).toHaveLength(2);
  expect(result.leonteqOnly.map((holding) => holding.name)).toEqual(['NVIDIA']);
  expect(result.airsOnly.map((holding) => holding.name)).toEqual(['Microsoft']);
});

it('preserves every duplicate line under its ISIN without a cartesian join or summing weights', () => {
  const left = [row(apple, 'Apple 1', '2%'), row(apple, 'Apple 2', '3%')];
  const right = [row(apple, 'Apple A', '1%'), row(apple, 'Apple B', '1.5%'), row(apple, 'Apple C', '2.5%')];
  const result = matchHoldings(left, right);
  expect(result.matched).toHaveLength(1);
  expect(result.matched[0].leonteq).toEqual(left);
  expect(result.matched[0].airs).toEqual(right);
  expect(result.leonteqOnly).toEqual([]);
  expect(result.airsOnly).toEqual([]);
});

it('never connects missing or invalid identifiers even when names match', () => {
  const positions = [row('', 'Unknown', '0%'), row('nan', 'Unknown'), row('EUR', 'Unknown')];
  const result = matchHoldings(positions, positions);
  expect(result.matched).toEqual([]);
  expect(result.leonteqOnly).toEqual(positions);
  expect(result.airsOnly).toEqual(positions);
});

it('sums currency cash weights against Effectenrekening without creating a cash trade', () => {
  const left = [row(null, 'EURO', '7.16%'), row(null, 'US DOLLAR', '0.16%')];
  const right = [row('CASH-IDENTIFIER', 'Effectenrekening', '6.9460999999999995%')];
  const result = matchHoldings(left, right);
  expect(result.leonteqOnly).toEqual([]);
  expect(result.airsOnly).toEqual([]);
  expect(result.matched[0].cash).toBe(true);
  expect(result.matched[0].leonteq[0].weight).toBe('7.32%');
  expect(result.matched[0].airs[0].weight).toBe('6.9461%');
  expect(rebalancePlan(left, right, [])).toEqual({ actions: [], issues: [] });
  expect(left[0].weight).toBe('7.16%');
});

it.each([
  ['US40415F1012', 'HDFC BANK LTD-ADR', 'INE040A01034', 'HDFC Bank INR'],
  ['JP3837800006', 'HOYA CORP', 'JP3786800000', 'HOYA Corporation'],
  ['USY5217N1183', 'LARSEN & TOUBRO-GDR REG S', 'INE018A01030', 'Larsen and Toubro'],
  ['US7960508882', 'SAMSUNG ELECTR-GDR', 'KR7005930003', 'Samsung Electron'],
  ['US78392B2060', 'SK HYNIX INC-ADR W/I', 'KR7000660001', 'SK Hynic'],
])('matches portfolio company exposure for %s while retaining both security identities', (leftIsin, leftName, rightIsin, rightName) => {
  const left = row(leftIsin, leftName, '5.08%'), right = row(rightIsin, rightName, '4.38%');
  const result = matchHoldings([left], [right]);
  expect(result.matched).toHaveLength(1);
  expect(result.matched[0].company).toBe(true);
  expect(result.matched[0].isin).toBe(`${leftIsin} ↔ ${rightIsin}`);
  expect(result.matched[0].leonteq).toEqual([left]);
  expect(result.matched[0].airs).toEqual([right]);
  expect(result.leonteqOnly).toEqual([]);
  expect(result.airsOnly).toEqual([]);
  expect(matchHoldings([left], [right], [{ leonteq_key: `isin:${leftIsin}`, airs_key: null }]).matched).toEqual([]);
});

it('does not guess between two company listings or between fund share classes', () => {
  expect(matchHoldings([row(apple, 'Example Inc')], [row(microsoft, 'Example Corp'), row(nvidia, 'Example Ltd')]).matched).toEqual([]);
  expect(matchHoldings([row(apple, 'Example Fund USD')], [row(microsoft, 'Example Fund EUR')]).matched).toEqual([]);
  expect(matchHoldings([row(apple, 'Example preferred')], [row(microsoft, 'Example')]).matched).toEqual([]);
});

it('blocks a sold marker when the issuer reappears through a depositary receipt', () => {
  const rules = [{ leonteq_key: 'sold:isin:INE040A01034', airs_key: 'isin:INE040A01034' }];
  const result = matchHoldings([row('US40415F1012', 'HDFC BANK LTD-ADR')], [row('INE040A01034', 'HDFC Bank INR')], rules);
  expect(result.matched).toEqual([]);
  expect(result.sold[0].conflict).toBe(true);
});
