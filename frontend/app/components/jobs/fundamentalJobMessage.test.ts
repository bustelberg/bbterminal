import { describe, expect, it } from 'vitest';

import { fundamentalJobMessage } from './fundamentalJobMessage';

describe('fundamentalJobMessage', () => {
  it('removes implementation details from an older backend failure', () => {
    expect(fundamentalJobMessage(
      'RuntimeError: Could not refresh 12 companies. '
      + 'ASR NEDERLAND NV: fin: GuruFocus did not provide financial statements. '
      + 'Please try again later. | IMCD NV: fin: GuruFocus did not provide financial statements. '
      + 'Please try again later. | KONINKLIJKE KPN NV: fin: GuruFocus did not provide '
      + 'financial statements. Please try again later.',
    )).toBe(
      'GuruFocus did not provide financial statements for 12 companies, including '
      + 'ASR NEDERLAND NV, IMCD NV and KONINKLIJKE KPN NV. Please try again later.',
    );
  });

  it('replaces long dash separators without shortening the message', () => {
    expect(fundamentalJobMessage('AEX — 2 companies refetched — complete')).toBe(
      'Checked 2 companies and updated the data GuruFocus supplied.',
    );
  });

  it('names the companies that could not be refreshed, without implementation counters', () => {
    expect(fundamentalJobMessage(
      'Bustelberg Offensief visible companies. 6 companies refetched, 1 failed, 20 data points, '
      + '54,395 already stored, 25 API calls · reported EPS 7/7 · FCF/share 3/7 · '
      + 'failures: BE Semiconductor Industries NV: GuruFocus did not provide financial statements. '
      + 'Please try again later. · prices: 7 refreshed (0 row(s))',
    )).toBe('Checked 6 companies and updated the data GuruFocus supplied. '
      + 'Could not refresh: BE Semiconductor Industries NV.');
  });

  it('uses a reader-facing status for an unchanged company', () => {
    expect(fundamentalJobMessage('[3/7] ASML Holding NV â€” no change (10,503 rows already stored)'))
      .toBe('[3/7] ASML Holding NV. up to date');
  });
});
