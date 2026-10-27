import { describe, expect, it } from 'vitest';

import { RISK_COPY, riskLongDate } from './riskCopy';

describe('Risk subtitle', () => {
  it('keeps the requested short heading in both languages', () => {
    expect(RISK_COPY.en.subtitle('Bustelberg Offensief', '2026-09-25')).toBe('Only stocks considered');
    expect(RISK_COPY.nl.subtitle('Bustelberg Neutraal', '2026-10-26')).toBe('Only stocks considered');
  });

  it('formats a date without applying a timezone', () => {
    expect(riskLongDate('2026-01-01T23:00:00Z')).toBe('1 januari 2026');
  });
});
