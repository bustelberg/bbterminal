import { describe, expect, it } from 'vitest';

import { RISK_COPY, riskLongDate } from './riskCopy';

describe('Risk subtitle', () => {
  it('names the AIRS portfolio and its valuation date', () => {
    expect(RISK_COPY.en.subtitle('Bustelberg Offensief', '2026-09-25')).toBe(
      'Individual stocks at their actual weight in AIRS for Bustelberg Offensief as of '
      + '25 september 2026.',
    );
  });

  it('formats a date without applying a timezone', () => {
    expect(riskLongDate('2026-01-01T23:00:00Z')).toBe('1 januari 2026');
  });
});
