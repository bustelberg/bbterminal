import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const source = readFileSync('app/components/PortfolioOverviewPanel.tsx', 'utf8');

describe('management dashboard account access', () => {
  it('keeps account rows from opening on click while retaining the explicit Analyse action', () => {
    expect(source).not.toContain('const expand = async');
    expect(source).not.toContain('<tr onClick={() => void expand(r.dynamic_portefeuille)}');
    expect(source).toContain('void openModal(r);');
  });
  it('keeps shared mapping edits separate from inspecting holdings', () => {
    expect(source).toContain('canEdit={isAdmin}');
    expect(source).toContain('{isOpen && (');
  });
});
