import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import BenchmarkMembershipMark from './BenchmarkMembershipMark';

describe('benchmark membership marker', () => {
  it('renders an accessible marker when the workbook-backed flag is true', () => {
    const html = renderToStaticMarkup(createElement(BenchmarkMembershipMark, { member: true, label: 'Also in ACWI' }));
    expect(html).toContain('aria-label="Also in ACWI"');
    expect(html).toContain('role="img"');
  });
  it('does not suggest membership for false or unavailable flags', () => {
    for (const member of [false, undefined]) {
      expect(renderToStaticMarkup(createElement(BenchmarkMembershipMark, { member, label: 'Also in ACWI' }))).toBe('');
    }
  });
});
