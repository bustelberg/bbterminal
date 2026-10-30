import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { expect, it, vi } from 'vitest';
import HoldingReturnExplanation from './HoldingReturnExplanation';
import type { ModelPortfolioAnalysis } from '../../../lib/types/api';

vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));
type Holding = NonNullable<ModelPortfolioAnalysis['book_holdings']>[number];
const base: Holding = { bucket: 'Equity', name: 'Alphabet - C', own_return_pct: 6.38, own_return_source: 'airs',
  own_return_from: '2026-01-01', own_return_as_of: '2026-10-23', own_return_book: 'Child portfolio' };

it('uses the holding’s own valuation date and book instead of the newer parent snapshot', () => {
  const html = renderToStaticMarkup(<HoldingReturnExplanation holding={base} asOf="2026-10-30" />);
  expect(html).toContain('+6.38%');
  expect(html).toContain('Child portfolio');
  expect(html).toContain('1 Jan 2026 – 23 Oct 2026');
  expect(html).not.toContain('30 Oct 2026');
});

it('does not date a Yahoo return with the AIRS snapshot and identifies estimated openings', () => {
  const html = renderToStaticMarkup(<HoldingReturnExplanation holding={{ ...base, own_return_source: 'yfinance',
    own_return_as_of: null, own_return_book: null, own_return_estimated: true }} asOf="2026-10-30" />);
  expect(html).toContain('Yahoo Finance');
  expect(html).toContain('date unavailable');
  expect(html).toContain('opening price is estimated');
  expect(html).not.toContain('30 Oct 2026');
});

it('does not invent the valuation date of another AIRS portfolio', () => {
  const html = renderToStaticMarkup(<HoldingReturnExplanation holding={{ ...base, own_return_as_of: null }}
    asOf="2026-10-30" parentBook="Parent portfolio" />);
  expect(html).toContain('date unavailable');
  expect(html).not.toContain('30 Oct 2026');
});

it('keeps an unavailable return distinct from a genuine zero return', () => {
  const html = renderToStaticMarkup(<HoldingReturnExplanation holding={{ ...base, own_return_pct: null }} />);
  expect(html).toContain('Not enough valuation data');
  expect(html).not.toContain('+0.00%');
});

it('keeps each blended source’s weight, return and date visible without a formula legend', () => {
  const html = renderToStaticMarkup(<HoldingReturnExplanation holding={{ ...base, sources: [
    { value_eur: 75, weight_now_pct: 75, book: 'Direct portfolio', blend_weight_pct: 75, return_pct: 5, as_of: '2026-10-29' },
    { value_eur: 25, weight_now_pct: 25, book: 'Child portfolio', blend_weight_pct: 25, return_pct: 10, as_of: '2026-10-23' },
  ] }} />);
  expect(html).toContain('75.00% × +5.00%');
  expect(html).toContain('25.00% × +10.00%');
  expect(html).toContain('29 Oct 2026');
  expect(html).toContain('23 Oct 2026');
});
