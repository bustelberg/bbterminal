import { describe, expect, it } from 'vitest';
import { ANALYSE_COPY } from './analyseCopy';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { benchmarkProvenance } from './benchmarkSourceNote';

function leaves(node: unknown, path = '', out: Record<string, string> = {}): Record<string, string> {
  if (typeof node === 'string') out[path] = node;
  else if (node && typeof node === 'object') {
    for (const [key, value] of Object.entries(node)) leaves(value, path ? `${path}.${key}` : key, out);
  }
  return out;
}

describe('Analyse primary-view copy', () => {
  const en = leaves(ANALYSE_COPY.en);
  const nl = leaves(ANALYSE_COPY.nl);

  it('has the same complete shape in both languages', () => {
    expect(Object.keys(nl).sort()).toEqual(Object.keys(en).sort());
    expect(Object.values(nl).filter((value) => !value.trim())).toEqual([]);
  });

  it('does not leave ordinary English labels in Dutch', () => {
    const sameByDesign = new Set([
      'chrome.benchmark', 'axes.sector', 'holdings.via', 'holdings.sector', 'holdings.momentum',
      'holdings.result', 'holdings.price', 'holdings.currency', 'holdings.rest',
      'holdings.direct', 'row.momentumNote',
    ]);
    expect(Object.keys(en).filter((key) => en[key] === nl[key] && !sameByDesign.has(key))).toEqual([]);
  });

  it('translates every asset class', () => {
    for (const name of ['Stocks', 'Bonds', 'Alternatives', 'Cash', 'Unclassified']) {
      expect(ANALYSE_COPY.nl.bucket(name)).not.toBe(name);
    }
  });

  it('keeps primary-view prose out of the component literals', () => {
    const source = readFileSync(join('app', 'components', 'portfolios', 'PortfolioAnalysisModal.tsx'), 'utf8');
    const forbidden = [
      'Sold during the year — no longer a holding', 'No valued positions to show here.',
      'How the Instrument return is built', 'What this portfolio returned year to date',
      'A dash is not a zero', 'No sector — a fund', 'The book’s year',
      'positions, everything it held or sold', 'Fundamental — is',
      'This index is rebuilt from', 'Why the trading mattered for',
    ];
    expect(forbidden.filter((phrase) => source.includes(phrase))).toEqual([]);
  });

  it('localises benchmark provenance too', () => {
    const card = benchmarkProvenance({ source: 'etf', label: 'ACWI', ticker: 'ACWI' }, 'nl');
    expect(card.what).toContain('eigen EUR-rendement');
    expect(card.note).toContain('sinds jaarbegin');
    expect(card.how).toContain('slot');
  });
});
