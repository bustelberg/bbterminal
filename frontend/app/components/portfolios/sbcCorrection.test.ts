import { describe, it, expect } from 'vitest';
import { correctedFcf, fcfLabel } from './sbcCorrection';

describe('correctedFcf', () => {
  it('subtracts SBC when the correction is on', () => {
    expect(correctedFcf(1000, 150, true)).toBe(850);
  });

  it('leaves FCF untouched when it is off', () => {
    expect(correctedFcf(1000, 150, false)).toBe(1000);
  });

  it('⚠ treats a MISSING SBC as zero, not as unknown', () => {
    // Deliberately asymmetric with how missing data is handled elsewhere here: most companies
    // genuinely report no stock compensation, so blanking their ratio would empty the chart for
    // the majority to be pedantic about the minority.
    expect(correctedFcf(1000, null, true)).toBe(1000);
    expect(correctedFcf(1000, undefined, true)).toBe(1000);
  });

  it('⚠ but a missing FCF is still null — that is the numerator', () => {
    expect(correctedFcf(null, 150, true)).toBeNull();
    expect(correctedFcf(undefined, 150, false)).toBeNull();
  });

  it('can drive the numerator negative, and does not clamp', () => {
    // A company whose stock comp exceeds its free cash flow is exactly what the correction exists
    // to expose; clamping at zero would hide it.
    expect(correctedFcf(100, 400, true)).toBe(-300);
  });

  it('is a no-op difference when a company pays no SBC', () => {
    expect(correctedFcf(500, 0, true)).toBe(correctedFcf(500, 0, false));
  });
});

describe('fcfLabel', () => {
  it('names the figure actually being drawn', () => {
    // The checkbox must not be the only clue: a screenshot of one card has to say which it is.
    expect(fcfLabel(true)).toBe('FCF-SBC');
    expect(fcfLabel(false)).toBe('FCF');
  });
});
