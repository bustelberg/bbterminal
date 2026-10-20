import { describe, expect, it } from 'vitest';
import { stockPriceTooltip } from './stockPriceTooltip';

const aligned = { modelPrice: 100, modelCurrency: 'EUR', modelDate: '2026-10-19',
  nativePrice: 110, nativeCurrency: 'USD', nativeDate: '2026-10-19' };

describe('stock price currency provenance', () => {
  it('shows the untouched Yahoo quote and actual applied conversion', () => {
    const text = stockPriceTooltip(aligned);
    expect(text).toContain('Original Yahoo close: 110 USD');
    expect(text).toContain('1 USD = 0.909091 EUR');
    expect(text).toContain('110 × 0.909091 ≈ 100 EUR');
  });
  it('does not invent FX when quote dates differ', () => {
    const text = stockPriceTooltip({ ...aligned, nativeDate: '2026-10-20' });
    expect(text).toContain('not date-aligned');
    expect(text).not.toContain('Applied conversion:');
  });
  it('handles same currency without conversion', () => {
    expect(stockPriceTooltip({ ...aligned, nativeCurrency: 'EUR', nativePrice: 100 })).toContain('No FX conversion needed');
  });
  it('never presents the native quote as a converted price when FX is missing', () => {
    expect(stockPriceTooltip({ ...aligned, modelPrice: null })).toContain('unconverted quote is never substituted');
  });
  it('includes pence-to-pound unit scaling in the applied factor', () => {
    expect(stockPriceTooltip({ ...aligned, modelCurrency: 'GBP', modelPrice: 80,
      nativeCurrency: 'GBp', nativePrice: 8000 })).toContain('1 GBp = 0.01 GBP');
  });
});
