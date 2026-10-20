import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import EtfComparison from './EtfComparison';
import { ETF_STARTERS } from './starters';

describe('the fixed ETF comparison rows', () => {
  it('shows all five named ETFs with linked tickers and independent date inputs before prices load', () => {
    const html = renderToStaticMarkup(createElement(EtfComparison));
    for (const etf of ETF_STARTERS) {
      expect(html).toContain(etf.name);
      expect(html).toContain(etf.ticker);
      expect(html).toContain(etf.isin);
      expect(html).toContain('https://finance.yahoo.com/quote/' + etf.ticker + '/');
    }
    expect((html.match(/type="date"/g) ?? []).length).toBe(5);
    expect((html.match(/role="status"/g) ?? []).length).toBe(5);
    expect(html).not.toContain('Remove ETF');
    expect(html).not.toContain('+ Add ETF');
    expect(html).toContain('lg:grid-cols-[18rem_minmax(0,1fr)]');
    expect(html).toContain('All returns are in EUR.');
    expect(html).toContain('historical FX rates for each observation');
    expect(html).not.toContain('without FX conversion');
  });
});
