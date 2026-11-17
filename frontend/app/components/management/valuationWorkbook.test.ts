import { expect, it } from 'vitest';
import { basisFx, workbookValuation, VALUATION_FORMULAS, type WorkbookRow } from './valuationWorkbook';
import { WORKBOOK_COLUMNS, WORKBOOK_PANELS, columnsForPanel, panelForInput } from './valuationWorkbookColumns';

const parameters = { years: 3, safety_margin: .2, bear_growth_delta: -.05,
  bear_multiple_delta: -.25, bull_growth_delta: .05, bull_multiple_delta: .25 };
const row: WorkbookRow = { isin: 'EXACT', ticker: 'OSTO:ADDT B', selections: ['ETS', 'STAR'], seed_source: 'test.xlsx',
  assumptions: { growth: .11, fair_multiple: 26, exit_multiple: 23.3, basis_kind: 'forward_eps' },
  forecast: { fy1: 10.28, fy2: 11.8, fy1_date: '2027-03-31', fy2_date: '2028-03-31', currency: 'SEK' } };
const quote = { price: 320.2, currency: 'SEK', price_date: '2026-11-06' };

it('uses saved EUR cross rates on or before the quote date, preserving overrides', () => {
  const converted = { ...row, forecast: { ...row.forecast, currency: 'USD' }, fx_rates: {
    '2026-11-05': { USD: 1.25, GBP: .9 }, '2026-11-07': { USD: 2 },
  } };
  expect(basisFx(converted, { ...quote, currency: 'EUR' })).toBe(.8);
  expect(basisFx(converted, { ...quote, currency: 'GBP' })).toBe(.72);
  expect(basisFx(converted, { ...quote, currency: 'EUR', price_date: '2026-11-16' })).toBeUndefined();
  expect(basisFx({ ...converted, assumptions: { ...row.assumptions, basis_fx: .85 } }, { ...quote, currency: 'EUR' })).toBe(.85);
  expect(workbookValuation(converted, { ...quote, currency: 'EUR' }, parameters).basis)
    .toBeCloseTo(workbookValuation(row, quote, parameters).basis! * .8);
});

it('reproduces the supplied Addtech workbook row, including upside rather than discount', () => {
  const result = workbookValuation(row, quote, parameters);
  expect(result.basis).toBeCloseTo(11.1666666667);
  expect(result.currentMultiple).toBeCloseTo(28.67462686);
  expect(result.fair).toBeCloseTo(290.33333333);
  expect(result.fairUpside).toBeCloseTo(-9.32750364);
  expect(result.buyMultiple).toBeCloseTo(20.8);
  expect(result.buyPrice).toBeCloseTo(232.26666667);
  expect(result.buyUpside).toBeCloseTo(-27.46200291);
  expect(result.bear).toBeCloseTo(232.4118847);
  expect(result.base).toBeCloseTo(355.83479235);
  expect(result.bull).toBeCloseTo(507.64890533);
  expect(result.cagr).toBeCloseTo(3.57994928);
  expect(result.signal).toBe('DO NOT BUY');
});

it('keeps zero growth and zero FY1 months, and leaves missing assumptions unavailable', () => {
  const result = workbookValuation({ ...row, assumptions: { ...row.assumptions, growth: 0, months_fy1: 0 } }, quote, parameters);
  expect(result.basis).toBe(11.8);
  expect(result.base).toBeCloseTo(11.8 * 23.3);
  const missing = workbookValuation({ ...row, assumptions: {} }, quote, parameters);
  expect(missing.fair).toBeNull(); expect(missing.base).toBeNull(); expect(missing.signal).toBe('?');
});

it('keeps special NAV/book-value bases explicit and converts pence once', () => {
  const nav = { ...row, assumptions: { ...row.assumptions, basis_kind: 'manual' as const,
    manual_basis: 3083, basis_currency: 'GBp', basis_label: 'NAV', fair_multiple: 1 } };
  const result = workbookValuation(nav, { ...quote, currency: 'GBP' }, parameters);
  expect(result.basis).toBe(30.830000000000002);
  expect(result.fair).toBeCloseTo(30.83);
  const mismatch = workbookValuation(row, { ...quote, currency: 'USD' }, parameters);
  expect(mismatch.basis).toBeNull(); expect(mismatch.reason).toContain('SEK → USD');
});

it('withholds buy signals for review flags, stale prices and nonpositive earnings', () => {
  expect(workbookValuation({ ...row, assumptions: { ...row.assumptions, review_required: true } }, { ...quote, price: 1 }, parameters).signal).toBe('REVIEW');
  expect(workbookValuation(row, { ...quote, price: 1 }, parameters, '2026-11-16').signal).toBe('?');
  expect(workbookValuation({ ...row, forecast: { ...row.forecast, fy1: -1, fy2: -2 } }, quote, parameters).fair).toBeNull();
});

it('allows every yellow formula to read only its declared blue/green inputs, never another yellow cell', () => {
  const kinds = new Map(WORKBOOK_COLUMNS.map(column => [column.key, column.kind]));
  kinds.set('as_of', 'data');
  for (const key of Object.keys(parameters)) kinds.set(key, 'assumption');
  const inputs = { raw_price: quote.price, quote_currency: quote.currency, price_date: quote.price_date, as_of: undefined,
    fy1: row.forecast.fy1, fy2: row.forecast.fy2, fy1_date: row.forecast.fy1_date, fy2_date: row.forecast.fy2_date,
    forecast_currency: row.forecast.currency, manual_basis: undefined, manual_currency: undefined,
    basis_kind: row.assumptions.basis_kind, basis_fx: undefined, months_fy1: undefined,
    growth: row.assumptions.growth, fair_multiple: row.assumptions.fair_multiple, exit_multiple: row.assumptions.exit_multiple,
    review_required: false, ...parameters };
  for (const formula of Object.values(VALUATION_FORMULAS)) {
    const allowed = new Set<string>(formula.inputs);
    expect([...allowed].every(key => kinds.get(key) === 'data' || kinds.get(key) === 'assumption')).toBe(true);
    for (const manual of [false, true]) {
      const guarded = new Proxy({ ...inputs, ...(manual ? { basis_kind: 'manual' as const,
        manual_basis: 25, manual_currency: 'SEK' } : {}) }, {
        get(target, key: string) {
          if (!allowed.has(key)) throw new Error(`Formula read an undeclared or calculated cell: ${key}`);
          return target[key as keyof typeof target];
        },
      });
      expect(() => formula.evaluate(guarded)).not.toThrow();
    }
  }
});

it('calculates from the raw pence quote without changing the blue source observation', () => {
  const sourceQuote = { price: 32020, currency: 'GBp', price_date: quote.price_date };
  const gbpRow = { ...row, forecast: { ...row.forecast, currency: 'GBP' } };
  const result = workbookValuation(gbpRow, sourceQuote, parameters);
  expect(sourceQuote.price).toBe(32020);
  expect(result.price).toBeCloseTo(320.2);
  expect(result.fairUpside).toBeCloseTo(-9.32750364);
});

it('keeps all source fields, assumptions and calculations accessible across the workbook tabs', () => {
  const visible = new Set(WORKBOOK_PANELS.flatMap(panel => columnsForPanel(panel.key).map(column => column.key)));
  const shared = WORKBOOK_COLUMNS.filter(column => !visible.has(column.key));
  expect(shared.map(column => column.key)).toEqual(['safety_margin', 'years']);
  for (const column of shared) expect(panelForInput(column.key)).toBe('parameters');
  for (const panel of WORKBOOK_PANELS) {
    const keys = columnsForPanel(panel.key).map(column => column.key);
    if (panel.key !== 'memberships' && panel.key !== 'parameters') expect(keys[0]).toBe('company');
    expect(new Set(keys).size).toBe(keys.length);
  }
});
