import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { expect, it, vi } from 'vitest';
import HoldingsColumnExplanation from './HoldingsColumnExplanation';

vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));

it('explains whole-portfolio weight using actual amounts and the valuation date', () => {
  const html = renderToStaticMarkup(<HoldingsColumnExplanation kind="weight" amount={20777}
    total={1277373} value={1.63} asOf="2026-10-30" />);
  expect(html).toContain('total includes cash');
  expect(html).toContain('€20,777.00 / €1,277,373.00 = 1.63%');
  expect(html).toContain('30 Oct 2026');
  expect(html).not.toContain('formula');
});

it('distinguishes percentage-point contribution from the investment’s own return', () => {
  const html = renderToStaticMarkup(<HoldingsColumnExplanation kind="contribution" amount={4017}
    total={1197811} value={0.34} asOf="2026-10-30" />);
  expect(html).toContain('percentage points');
  expect(html).toContain('€4,017.00 / €1,197,811.00 = +0.34pp');
});

it('keeps missing data distinct from zero and never displays division by zero', () => {
  const missing = renderToStaticMarkup(<HoldingsColumnExplanation kind="contribution" value={null} />);
  expect(missing).toContain('Not enough data');
  expect(missing).not.toContain('+0.00pp');
  const zero = renderToStaticMarkup(<HoldingsColumnExplanation kind="contribution" amount={0} total={0} value={0} />);
  expect(zero).toContain('+0.00pp');
  expect(zero).not.toContain(' / ');
});
