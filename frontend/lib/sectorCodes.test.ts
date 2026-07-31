import { describe, it, expect } from 'vitest';
import { SECTOR_CODES, inkForBackground, sectorCode } from './sectorCodes';
import { SECTOR_COLORS, colorForSector } from './sectorColors';

describe('sectorCode — the secondary encoding', () => {
  it('⚠ every mapped code is UNIQUE per colour', () => {
    // The code exists because two colour pairs are indistinguishable. If two sectors that carry
    // DIFFERENT colours shared a code, the chip would be ambiguous on both channels at once.
    const byCode = new Map<string, Set<string>>();
    for (const [sector, code] of Object.entries(SECTOR_CODES)) {
      const set = byCode.get(code) ?? new Set<string>();
      set.add(colorForSector(sector));
      byCode.set(code, set);
    }
    const clashes = [...byCode.entries()].filter(([, colors]) => colors.size > 1);
    expect(clashes).toEqual([]);
  });

  it('⚠ one letter would NOT have been enough', () => {
    // Technology/Transportation, and Communication/Consumer*/Capital Goods, collide on letter 1.
    const firsts = Object.keys(SECTOR_CODES).map((s) => s[0]);
    expect(new Set(firsts).size).toBeLessThan(firsts.length);
  });

  it('the two taxonomies share a code, exactly as they share a colour', () => {
    expect(sectorCode('Information Technology')).toBe(sectorCode('Technology'));
    expect(sectorCode('Health Care')).toBe(sectorCode('Healthcare'));
    expect(sectorCode('Basic Materials')).toBe(sectorCode('Materials'));
  });

  it('the pairs the validator FAILS are separable by code', () => {
    // Services <-> Energy: dE 3.5 normal vision. Industrials <-> Technology: dE 0.9 deutan.
    expect(sectorCode('Services')).not.toBe(sectorCode('Energy'));
    expect(sectorCode('Industrials')).not.toBe(sectorCode('Technology'));
  });

  it('falls back to the first two letters for an unmapped sector', () => {
    expect(sectorCode('Widgets & Sprockets')).toBe('WI');
  });

  it('never renders an empty chip', () => {
    expect(sectorCode(null)).toBe('—');
    expect(sectorCode('')).toBe('—');
    expect(sectorCode('  ')).toBe('—');
    expect(sectorCode('!!')).toBe('—');
  });
});

describe('inkForBackground — text on a filled chip', () => {
  it('⚠ picks DARK ink on the light half of the palette', () => {
    // Utilities (#fbbf24) and Materials (#84cc16) are the two the validator flags at 1.63 and
    // 1.92 contrast against white — white text on them is unreadable.
    expect(inkForBackground(SECTOR_COLORS.Utilities)).toBe('#111827');
    expect(inkForBackground(SECTOR_COLORS.Materials)).toBe('#111827');
  });

  it('picks light ink on the dark half', () => {
    expect(inkForBackground(SECTOR_COLORS['Consumer Staples'])).toBe('#ffffff');
    expect(inkForBackground(SECTOR_COLORS.Energy)).toBe('#ffffff');
  });

  it('every sector colour gets legible ink rather than a default', () => {
    for (const hex of new Set(Object.values(SECTOR_COLORS))) {
      expect(['#111827', '#ffffff']).toContain(inkForBackground(hex));
    }
  });

  it('a malformed colour falls back to dark ink rather than throwing', () => {
    expect(inkForBackground('not-a-color')).toBe('#111827');
    expect(inkForBackground('')).toBe('#111827');
  });

  it('accepts a hex with or without the leading #', () => {
    expect(inkForBackground('fbbf24')).toBe(inkForBackground('#fbbf24'));
  });
});
