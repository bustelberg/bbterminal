import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { expect, it } from 'vitest';
import { Names } from './AttributionPanel';

it('preserves the ACWI marker in contributor/detractor summary tables, even without a return', () => {
  const html = renderToStaticMarkup(createElement(Names, {
    title: 'Contributors', benchmark: 'ACWI', weightSrc: 'derived', returnSrc: 'derived',
    weightHow: 'Opening book weight', returnHow: 'Price return',
    rows: [{ isin: 'US78409V1044', name: 'S&P Global Inc', weight_pct: 1.37, return_pct: null, in_both: true }],
  }));
  expect(html).toContain('S&amp;P Global Inc');
  expect(html).toContain('role="img"');
  expect(html).toContain('aria-label=');
  expect(html).toContain('ACWI');
});
