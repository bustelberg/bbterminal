// @vitest-environment happy-dom
import React, { act, type ReactNode } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { apiFetch } from '../../../lib/apiFetch';
import { CompanyNotesModal } from './PortfolioAnalysisModal';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('./PanelDialog', () => ({ default: ({ children }: { children: ReactNode }) => <div>{children}</div> }));

it.each([null, 42])('loads and saves Notes by ISIN with company_id=%s', async companyId => {
  vi.mocked(apiFetch).mockReset();
  const note = { thesis: 'Existing private research', pillars: ['Recurring revenue'] };
  vi.mocked(apiFetch).mockImplementation(async () => new Response(JSON.stringify(note)));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); const root = createRoot(host);
  try {
    await act(async () => root.render(<CompanyNotesModal holding={{ isin: 'US18507C1036', name: 'ClearPoint Neuro',
      bucket: 'Equity', company_id: companyId }} onClose={() => {}} />));
    expect(host.querySelector('textarea')?.value).toBe(note.thesis);
    expect(host.querySelector('textarea')?.disabled).toBe(false);
    await act(async () => [...host.querySelectorAll('button')].find(button => button.textContent === 'Save summary')!.click());
    expect(apiFetch).toHaveBeenLastCalledWith(expect.stringContaining('/by-isin/US18507C1036/investment-note'),
      expect.objectContaining({ method: 'PUT', body: JSON.stringify(note) }));
    expect(host.textContent).toContain('Saved');
  } finally { await act(async () => root.unmount()); }
});
