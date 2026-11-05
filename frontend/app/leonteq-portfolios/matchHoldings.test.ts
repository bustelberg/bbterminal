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

it('lets manual connections override ISIN matches, including cash without an ISIN', () => {
  const result = matchHoldings([row(apple, 'APPLE'), row(microsoft, 'MSFT'), row(null, 'EURO')],
    [row(microsoft, 'Microsoft'), row(null, 'Liquiditeiten')], [
      { leonteq_key: `isin:${apple}`, airs_key: `isin:${microsoft}` },
      { leonteq_key: 'name:euro', airs_key: 'name:liquiditeiten' },
    ]);
  expect(result.matched).toHaveLength(2);
  expect(result.matched.every((match) => match.manual)).toBe(true);
  expect(result.leonteqOnly.map((holding) => holding.name)).toEqual(['MSFT']);
  expect(result.airsOnly).toEqual([]);
});

it('does not silently rematch a manual selection when its target disappears', () => {
  const result = matchHoldings([row(apple, 'Apple')], [row(apple, 'Apple')], [{ leonteq_key: `isin:${apple}`, airs_key: `isin:${nvidia}` }]);
  expect(result.matched).toEqual([]);
});

it('keeps different ISINs and cash in their respective leftovers', () => {
  const left = [row(apple, 'Apple'), row(nvidia, 'NVIDIA'), row(null, 'EURO')];
  const right = [row(apple, 'Apple'), row(microsoft, 'Microsoft'), row(null, 'Liquiditeiten')];
  const result = matchHoldings(left, right);
  expect(result.matched).toHaveLength(1);
  expect(result.leonteqOnly.map((holding) => holding.name)).toEqual(['NVIDIA', 'EURO']);
  expect(result.airsOnly.map((holding) => holding.name)).toEqual(['Microsoft', 'Liquiditeiten']);
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
  const positions = [row('', 'Cash', '0%'), row('nan', 'Cash'), row('EUR', 'Cash')];
  const result = matchHoldings(positions, positions);
  expect(result.matched).toEqual([]);
  expect(result.leonteqOnly).toEqual(positions);
  expect(result.airsOnly).toEqual(positions);
});
