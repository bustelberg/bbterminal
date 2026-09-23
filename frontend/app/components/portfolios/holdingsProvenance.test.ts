import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const FILE = join('app', 'components', 'portfolios', 'BucketDetailPanel.tsx');
const ATTRIBUTION_FILE = join('app', 'components', 'portfolios', 'AttributionPanel.tsx');

function source(): string {
  return readFileSync(FILE, 'utf8')
    .replace(/\{\/\*[\s\S]*?\*\/\}/g, '')
    .replace(/\/\*[\s\S]*?\*\//g, '');
}

describe('the per-company holdings table explains every displayed cell', () => {
  it('puts a What/Where/When/How provenance icon in every descriptive column header', () => {
    const s = source();
    const head = s.slice(s.indexOf('<thead>'), s.indexOf('</thead>'));
    expect((head.match(/<Provenance\b/g) ?? []).length).toBe(5);
    expect(head).toContain('<th className="pr-1 text-right font-normal">#</th>');
  });

  it('puts one provenance icon beside name, both weights, return and contribution, but not rank', () => {
    const s = source();
    const start = s.indexOf('{sorted.map((h, i) => (');
    const row = s.slice(start, s.indexOf('</tr>', start));
    expect((row.match(/<Provenance\b/g) ?? []).length).toBe(1); // company name
    expect(row).toContain('>{i + 1}</td>');
    expect((row.match(/weightProv\(/g) ?? []).length).toBe(2);
    expect((row.match(/returnProv\(/g) ?? []).length).toBe(1);
    expect((row.match(/contributionProv\(/g) ?? []).length).toBe(1);
  });

  it('also explains every non-empty total cell', () => {
    const s = source();
    const start = s.indexOf('<tr className="border-t border-neutral-800/40 bg-inset');
    const total = s.slice(start, s.indexOf('</tr>', start));
    expect((total.match(/<Provenance\b/g) ?? []).length).toBe(1); // aggregate label
    expect((total.match(/weightProv\(/g) ?? []).length).toBe(2);
    expect((total.match(/returnProv\(/g) ?? []).length).toBe(1);
    expect((total.match(/contributionProv\(/g) ?? []).length).toBe(1);
  });

  it('explains blue overlap circles before either holdings list', () => {
    for (const file of [FILE, ATTRIBUTION_FILE]) {
      const s = readFileSync(file, 'utf8');
      const legend = file === FILE ? s.indexOf('{t.overlapLegend(benchmark)}')
        : s.indexOf('{copy.names.shared(benchmark)}');
      const lists = s.indexOf('lg:grid-cols-2', legend);
      expect(legend).toBeGreaterThan(-1);
      expect(lists).toBeGreaterThan(legend);
    }
  });
});
