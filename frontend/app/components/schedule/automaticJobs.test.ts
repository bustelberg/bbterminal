import { describe, expect, it } from 'vitest';
import { summaryLine } from './automaticJobs';

describe('scheduled job receipt summary', () => {
  it('keeps counts and simple failure reasons readable', () => {
    expect(summaryLine({ filled: 4, errors: ['missing statements'], empty: [] }))
      .toBe('filled 4 · errors missing statements');
  });

  it('summarizes nested receipts without coercing objects to strings', () => {
    const line = summaryLine({
      accounts: [{ portefeuille: 'BOOK', issues: ['missing close'] }],
      errors: { audit: { receipt: { incomplete: 1 } } },
      fx_dates: { USD: '2026-10-30', GBP: '2026-10-30' },
    });
    expect(line).toBe('accounts 1 entries · errors 1 entries · fx dates 2 entries');
    expect(line).not.toContain('[object Object]');
  });
});
