import katex from 'katex';
import { describe, expect, it } from 'vitest';

import { moneyWeightedWorked } from './MoneyWeightedCalculation';

describe('moneyWeightedWorked', () => {
  it('typesets the actual dated AIRS operands and the holding-period result', () => {
    const tex = moneyWeightedWorked([
      { date: '2026-01-01', amount_eur: -1000, kind: 'opening value', source: 'AIRS VOLK' },
      { date: '2026-03-01', amount_eur: -250, kind: 'purchase', source: 'AIRS Transacties' },
      { date: '2026-09-24', amount_eur: 1600, kind: 'final valuation', source: 'AIRS VOLK' },
    ], 30.25)!;

    expect(tex).toContain('-\\frac{\\mathrm{EUR}\\,1{,}000.00}{(1+r)^{0/365}}');
    expect(tex).toContain('-\\frac{\\mathrm{EUR}\\,250.00}{(1+r)^{59/365}}');
    expect(tex).toContain('+\\frac{\\mathrm{EUR}\\,1{,}600.00}{(1+r)^{266/365}}');
    expect(tex).toContain('=+30.25\\%');
    expect(() => katex.renderToString(tex, {
      displayMode: true, throwOnError: true, strict: 'error',
    })).not.toThrow();
  });
});
