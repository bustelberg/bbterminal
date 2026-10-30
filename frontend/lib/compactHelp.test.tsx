import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it, vi } from 'vitest';
import { CompactHelp, CompactHelpProvider, workedExample } from './compactHelp';
import { AboutCard, AspectCard, TipCardLanguageProvider } from './tipCard';
import { Provenance } from './provenance';
import { withWorked } from '../app/components/portfolios/workedFormula';

vi.mock('./i18n', () => ({ useLang: () => ['en'] }));
vi.mock('../app/components/InfoTip', () => ({ default: ({ content }: { content: React.ReactNode }) => <>{content}</> }));

describe('dashboard tooltip presentation', () => {
  it('keeps normal pages unchanged and removes About chrome only inside the dashboard scope', () => {
    expect(renderToStaticMarkup(<AboutCard text="A brief explanation." />)).toContain('About');
    const compact = renderToStaticMarkup(<CompactHelpProvider><AboutCard text="A brief explanation." /></CompactHelpProvider>);
    expect(compact).toContain('A brief explanation.');
    expect(compact).not.toContain('About');
  });

  it('keeps an exact numerical worked example and retains all notation in optional details', () => {
    const worked = withWorked(String.raw`\sum_i w_i R_i`, '0.6 \\times 1.38 + 0.4 \\times 0.04 = 0.844');
    expect(workedExample(worked)).toBe('0.6 \\times 1.38 + 0.4 \\times 0.04 = 0.844');
    expect(workedExample('not a withWorked expression')).toBeNull();
    const html = renderToStaticMarkup(<CompactHelpProvider><AspectCard what="The policy-weighted return."
      worked={worked} how="Multiply each ETF return by its policy weight."
      where="Stored EUR prices" when="31 Dec 2025 – 30 Jan 2026" /></CompactHelpProvider>);
    expect(html).toContain('Calculation details');
    expect(html).toContain('<details');
    expect(html).not.toContain('<details open');
    expect(html).toContain('Stored EUR prices');
    expect(html).toContain('31 Dec 2025');
    expect(html).not.toContain('>What<');
    expect(html).not.toContain('>Where<');
  });

  it('preserves caller-designed tables and their actual values', () => {
    const html = renderToStaticMarkup(<CompactHelp summary="ETF prices" calculation={<table><tbody>
      <tr><td>ACWI</td><td>€119.64</td><td>€121.29</td></tr>
    </tbody></table>} />);
    expect(html).toContain('<table>');
    expect(html).toContain('€119.64');
    expect(html).toContain('€121.29');
  });

  it('shows an already short method directly without requiring a click', () => {
    const html = renderToStaticMarkup(<CompactHelp summary="Portfolio weight" method="Position value divided by total portfolio value." />);
    expect(html).toContain('Position value divided by total portfolio value.');
    expect(html).not.toContain('<details');
  });

  it('preserves the language selected by a nested translated dialog', () => {
    const html = renderToStaticMarkup(<CompactHelpProvider><TipCardLanguageProvider lang="nl">
      <AspectCard what="Gewogen rendement" worked={withWorked('x', '1 + 2 = 3')} />
    </TipCardLanguageProvider></CompactHelpProvider>);
    expect(html).toContain('Berekening en toelichting');
    expect(html).not.toContain('Calculation details');
  });

  it('keeps acquisition failure warnings and distinguishes valuation date from fetch date', () => {
    const html = renderToStaticMarkup(<CompactHelpProvider><Provenance source="airs_volk"
      what="Current portfolio value." asOf="2026-10-29" fetchedAt="2026-10-30T11:00:00Z"
      staleReason="Some holdings failed to refresh" how="Sum the current values.\n\n€40 + €60 = €100" kind="formula" />
    </CompactHelpProvider>);
    expect(html).toContain('2026-10-29');
    expect(html).toContain('2026-10-30');
    expect(html).toContain('Some holdings failed to refresh');
    expect(html).toContain('€40 + €60 = €100');
    expect(html).not.toContain('A formula on the data');
  });
});
