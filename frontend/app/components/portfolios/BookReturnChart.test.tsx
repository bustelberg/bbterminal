// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { apiFetch } from '../../../lib/apiFetch';
import type { BookValueSeries } from '../../../lib/types/api';
import BookReturnChart from './BookReturnChart';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));

it('uses bundled chart data without another request, including refresh and unavailable results', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  const root = createRoot(host);
  const series = { returns: [], reason: 'No AIRS observations yet.' } as unknown as BookValueSeries;
  try {
    await act(async () => root.render(<BookReturnChart portfolioId={1} benchmark="ACWI" series={series} />));
    expect(host.textContent).toContain('No AIRS observations yet.');
    await act(async () => root.render(<BookReturnChart portfolioId={1} benchmark="ACWI" refreshSeq={1}
      series={{ ...series, reason: 'History updated.' }} />));
    expect(host.textContent).toContain('History updated.');
    await act(async () => root.render(<BookReturnChart portfolioId={1} benchmark="ACWI" series={null} />));
    expect(host.textContent).toContain('No return available.');
    expect(apiFetch).not.toHaveBeenCalled();
  } finally {
    await act(async () => root.unmount());
  }
});
