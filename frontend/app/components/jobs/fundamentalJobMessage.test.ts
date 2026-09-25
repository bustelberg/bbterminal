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
      'AEX. 2 companies refetched. complete',
    );
  });
});
