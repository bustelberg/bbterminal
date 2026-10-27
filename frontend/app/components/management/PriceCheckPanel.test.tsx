import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { expect, it, vi } from 'vitest';
import { PriceCheckBadge, PriceCheckDetails } from './PriceCheckPanel';
import { summarizePrices } from './priceCheck';

vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));

const summary = (yahooDate: string) => summarizePrices({ portefeuille: 'BOOK', as_of: '2026-10-27', rows: [{
  holding_name: 'Stock', isin: 'TEST', bucket: 'Equity', verdict: 'price_mismatch',
  currency: 'EUR', our_price_currency: 'EUR', airs_price_local: 103.12,
  our_price_local: 97.12, our_price_date: yahooDate,
}] });

it('does not prompt users to refresh a date-only gap', () => {
  const state = { summary: summary('2026-10-26'), pending: false, error: false };
  expect(renderToStaticMarkup(<PriceCheckBadge state={state} onClick={() => {}} />)).toBe('');
  expect(renderToStaticMarkup(<PriceCheckDetails state={state} onRetry={() => {}} />)).toBe('');
  const full = renderToStaticMarkup(<PriceCheckDetails state={state} onRetry={() => {}} showAll />);
  expect(full).toContain('checked automatically');
  expect(full).not.toContain('Refresh needed');
});

it('keeps same-date discrepancies visible', () => {
  const state = { summary: summary('2026-10-27'), pending: false, error: false };
  expect(renderToStaticMarkup(<PriceCheckBadge state={state} onClick={() => {}} />)).toContain('1 difference');
  expect(renderToStaticMarkup(<PriceCheckDetails state={state} onRetry={() => {}} />)).toContain('Stock');
});
