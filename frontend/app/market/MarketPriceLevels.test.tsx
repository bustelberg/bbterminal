import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { expect, it, vi } from 'vitest';
import MarketPriceLevels from './MarketPriceLevels';

vi.mock('../../lib/i18n', () => ({ useLang: () => ['en'] }));

it('shows saved personal levels, their target prices, and their current hit status', () => {
  const html = renderToStaticMarkup(<MarketPriceLevels market="nasdaq" name="Nasdaq" peak={200} latest={178} currency="USD"
    levels={{ rows: [{ market: 'nasdaq', drawdown_pct: 10 }, { market: 'nasdaq', drawdown_pct: 20 },
      { market: 'acwi', drawdown_pct: 5 }], ready: true, saving: false, error: false,
      update: vi.fn(), retry: vi.fn() }} />);
  for (const text of ['180.00', '160.00', 'Hit now', 'Not hit', 'Below ATH (%)', 'Remove 10% · Nasdaq']) expect(html).toContain(text);
  expect(html).not.toContain('190.00');
});
