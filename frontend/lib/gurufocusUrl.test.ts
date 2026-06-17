import { describe, it, expect } from 'vitest';
import { guruFocusUrl } from './gurufocusUrl';

describe('guruFocusUrl', () => {
  it('US exchanges produce a bare URL', () => {
    expect(guruFocusUrl('AAPL', 'NASDAQ')).toBe('https://www.gurufocus.com/stock/AAPL/summary');
  });

  it('foreign exchanges get a prefix', () => {
    expect(guruFocusUrl('NESN', 'XSWX')).toBe('https://www.gurufocus.com/stock/XSWX:NESN/summary');
  });

  describe('HKSE zero-pad', () => {
    it('pads a 1-digit ticker to 5 (CK Hutchison)', () => {
      expect(guruFocusUrl('1', 'HKSE')).toBe('https://www.gurufocus.com/stock/HKSE:00001/summary');
    });

    it('pads a 3-digit ticker', () => {
      expect(guruFocusUrl('700', 'HKSE')).toBe('https://www.gurufocus.com/stock/HKSE:00700/summary');
    });

    it('leaves an already-padded ticker unchanged', () => {
      expect(guruFocusUrl('00700', 'HKSE')).toBe('https://www.gurufocus.com/stock/HKSE:00700/summary');
    });

    it('infers HKSE from a 4-5 digit ticker with no exchange, padded', () => {
      // 4-digit numeric with empty exchange → inferred HKSE, padded to 5.
      expect(guruFocusUrl('1988', '')).toBe('https://www.gurufocus.com/stock/HKSE:01988/summary');
    });

    it('does not pad numeric tickers on other exchanges', () => {
      expect(guruFocusUrl('700', 'XTKS')).toBe('https://www.gurufocus.com/stock/XTKS:700/summary');
    });
  });
});
