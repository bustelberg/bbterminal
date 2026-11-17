import katex from 'katex';
import { expect, it } from 'vitest';
import { VALUATION_FORMULAS, type CalculationKey, type Workbook, type WorkbookRow } from './valuationWorkbook';
import { valuationFormulaLatex } from './valuationFormulaLatex';

const row: WorkbookRow = { isin: 'AMD', ticker: 'AMD', selections: [], seed_source: null,
  assumptions: { fair_multiple: 24, growth: .1, exit_multiple: 20 },
  forecast: { fy1: 8.06, fy2: 17.76, fy1_date: '2026-12-31', fy2_date: '2027-12-31', currency: 'USD' },
  quote: { price: 200, currency: 'USD', price_date: '2026-11-16' } };
const workbook: Workbook = { portfolio_id: 1, rows: [row], as_of: '2026-11-17', parameters: {
  years: 3, safety_margin: .2, bear_growth_delta: -.05, bear_multiple_delta: -.25, bull_growth_delta: .05, bull_multiple_delta: .25 } };

it.each(Object.keys(VALUATION_FORMULAS) as CalculationKey[])('renders valid symbolic and substituted LaTeX for %s', key => {
  for (const example of [row, { ...row, assumptions: { ...row.assumptions, basis_kind: 'manual' as const,
    manual_basis: 500, basis_currency: 'GBp', basis_fx: 1.3 } }]) {
    const formula = valuationFormulaLatex(key, workbook, example);
    for (const tex of [formula.symbolic, formula.substituted].filter((value): value is string => !!value)) {
      expect(() => katex.renderToString(tex, { throwOnError: true, strict: 'error' })).not.toThrow();
    }
  }
});

it('substitutes AMD forecasts, two calendar months and the multiple to reach 387.44 USD', () => {
  const formula = valuationFormulaLatex('fair', workbook, row);
  expect(formula.symbolic).toContain('EPS_1');
  expect(formula.variables.map(item => item.symbol)).toEqual(['EPS_1', 'EPS_2', 'M', 's_B', 'FX', 'm_f']);
  expect(formula.substituted).toContain('8.06\\frac{2}{12}');
  expect(formula.substituted).toContain('17.76\\frac{12-2}{12}');
  expect(formula.substituted).toContain('\\times 24 = 387.44\\;\\text{USD}');
});

it('does not invent a result when required data is missing', () => {
  expect(valuationFormulaLatex('fair', workbook, { ...row, forecast: {} }).substituted).toBeNull();
});

it('shows at most two decimals in substituted values without changing calculation precision', () => {
  const formula = valuationFormulaLatex('fair', workbook, { ...row, forecast: { ...row.forecast, fy1: 8.067891 } });
  expect(formula.substituted).toContain('8.07\\frac{2}{12}');
  expect(formula.substituted).not.toMatch(/\d+\.\d{3}/);
  expect(formula.substituted).toContain('\\approx 387.47');
});

it('defines only the variables used by manual and calendar-month formulas', () => {
  const manual = { ...row, assumptions: { ...row.assumptions, basis_kind: 'manual' as const, manual_basis: 5, basis_currency: 'USD' } };
  expect(valuationFormulaLatex('fair', workbook, manual).variables.map(item => item.symbol))
    .toEqual(['B_{\\mathrm{manual}}', 's_B', 'FX', 'm_f']);
  expect(valuationFormulaLatex('calendarMonths', workbook, row).variables.map(item => item.symbol))
    .toEqual(['Y_1', 'Y_P', 'M_1', 'M_P']);
});
