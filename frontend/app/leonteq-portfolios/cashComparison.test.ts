import { expect, it } from 'vitest';
import { cashComparison } from './cashComparison';
import type { CompositionResult } from './CompositionTable';
import type { AirsComposition } from './AirsCompositionView';

const position = (name: string, weight: string | null) => ({ name, weight, isin: null, total_value: null, units: null, price: null, currency: null, performance: null });
const leonteq: CompositionResult = { success: true, status: 'success', groups: [{ name: 'Cash', weight: '7.32%', positions: [position('EURO', '7.16%'), position('US DOLLAR', '0.16%')] }] };
const airs: AirsComposition = { portfolio_id: 1, portfolio: 'Fixed', datum: '2026-11-10', dates: [], rows: [{ id: 1, Fonds: 'Effectenrekening', Percentage: 6.94, ISINCode: null, valuta: null, Beleggingscategorie: null, Beleggingssector: null, regio: null, afmCategorie: null }] };

it('combines cash currencies and reports the signed percentage point difference', () => {
  const result = cashComparison(leonteq, airs);
  expect(result.leonteq).toBeCloseTo(7.32);
  expect(result.airs).toBe(6.94);
  expect(result.difference).toBeCloseTo(-0.38);
});

it('keeps missing cash distinct from an explicit zero', () => {
  expect(cashComparison(undefined, airs).difference).toBeNull();
  expect(cashComparison(leonteq, { ...airs, rows: [] }).airs).toBeNull();
  const empty = { ...leonteq, groups: [{ name: 'Cash', weight: '0%', positions: [] }] };
  expect(cashComparison(empty, airs).leonteq).toBe(0);
  const missing = { ...leonteq, groups: [{ ...leonteq.groups![0], positions: [position('EURO', null)] }] };
  expect(cashComparison(missing, airs).leonteq).toBeNull();
});
