import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const source = readFileSync('app/components/PortfolioOverviewPanel.tsx', 'utf8');

describe('management dashboard account access', () => {
  it('allows a regular user to expand account detail instead of silently ignoring clicks', () => {
    const expand = source.match(/const expand = async \(p: string\) => \{([\s\S]*?)\n  \};/)?.[1];
    expect(expand).toBeDefined();
    expect(expand).not.toContain('isAdmin');
    expect(source).toContain('<tr onClick={() => void expand(r.dynamic_portefeuille)}');
  });
  it('keeps shared mapping edits separate from inspecting holdings', () => {
    expect(source).toContain('canEdit={isAdmin}');
    expect(source).toContain('{isOpen && (');
  });
});
