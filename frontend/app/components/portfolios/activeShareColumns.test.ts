import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const source = readFileSync(new URL('./ActiveSharePanel.tsx', import.meta.url), 'utf8');

describe('Active share header widths', () => {
  it('sizes portfolio and benchmark columns for their full labels and keeps all headers on one line', () => {
    expect(source).toContain('portfolioName.length + 6');
    expect(source).toContain('(data?.benchmark ?? benchmark).length + 6');
    expect(source).toContain('[&>th]:whitespace-nowrap');
    expect(source).not.toContain('<col className="w-20"');
  });

  it('keeps rows and pinned totals on shared columns and one horizontal scroll surface', () => {
    expect(source.match(/\{tableColumns\}/g)).toHaveLength(2);
    expect(source).toContain('style={{ minWidth: tableMinWidth }}');
    expect(source).toContain('overflow-x-auto');
    expect(source).toContain('aria-label="Active share totals"');
  });
});
