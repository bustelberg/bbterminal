// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { apiFetch } from '../../lib/apiFetch';
import MonthlyReturns, { monthlyFailure } from './MonthlyReturns';
vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
it('explains source failures, login errors and access errors', () => {
  expect(monthlyFailure({ success: true, refresh_errors: [{ source: 'AIRS', code: 'airs_busy' }, { source: 'Leonteq', code: 'timeout' }] })).toContain('AIRS is busy');
  expect(monthlyFailure({ success: false, status: 'login_rejected' })).toContain('Leonteq: Login rejected');
  expect(monthlyFailure({ success: false }, 403)).toContain('Access denied');
});
it('keeps failure text short and copies descriptive JSON', async () => {
  const writeText = vi.fn().mockResolvedValue(undefined);
  const original = Object.getOwnPropertyDescriptor(navigator, 'clipboard');
  Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } });
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<MonthlyReturns link={{ leonteq_name: 'Source', airs_name: 'Dynamic', airs_portfolio_id: 2 }}
      fresh={{ success: true, months: [], refresh_error: 'airs_busy' }} refreshState="Failed"
      refreshError="AIRS is busy with another refresh. Retry shortly." errorReport={{ http_status: 200, response: { refresh_errors: [{ source: 'AIRS', code: 'airs_busy' }] } }} />));
    expect(host.querySelector('[role=alert]')!.textContent).toBe('Failed');
    expect(host.textContent!.match(/Failed/g)).toHaveLength(1);
    expect(host.querySelector('[role=alert]')!.previousElementSibling?.textContent).toBe('Copy .json');
    expect(host.textContent).not.toContain('Retry shortly');
    await act(async () => host.querySelector('button')!.click());
    const copied = JSON.parse(writeText.mock.calls[0][0]);
    expect(copied.message).toContain('AIRS is busy');
    expect(copied.airs_portfolio_id).toBe(2);
    expect(copied.diagnostics.response.refresh_errors[0].code).toBe('airs_busy');
    expect(host.textContent).toContain('Copied');
  } finally {
    await act(async () => root.unmount()); host.remove();
    if (original) Object.defineProperty(navigator, 'clipboard', original);
    else Reflect.deleteProperty(navigator, 'clipboard');
  }
});
it('renders monthly returns and gaps, preserving zero and refreshing only on request', async () => {
  vi.mocked(apiFetch).mockResolvedValue({ ok: true, json: async () => ({ success: true, months: [
    { month: '2026-11', leonteq_pct: 2, airs_pct: 1.5, difference_pp: .5, airs_as_of: '2026-11-04' },
    { month: '2026-10', leonteq_pct: 0, airs_pct: null, difference_pp: null },
  ], ytd: { '2026': { airs_pct: 6.68, leonteq_pct: 7.68, difference_pp: 1 } } }) } as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<MonthlyReturns link={{ leonteq_name: 'Source', airs_name: 'Dynamic', airs_portfolio_id: 2 }} />));
    expect(host.textContent).toContain('+0.50 pp');
    expect(host.textContent).toContain('0.00%');
    expect(host.textContent).toContain('—');
    const rows = host.querySelectorAll('tbody tr');
    expect(rows[0].textContent).toContain('AIRS');
    expect(rows[1].textContent).toContain('Leonteq');
    expect(rows[2].textContent).toContain('Difference');
    expect(rows[0].querySelector('td:last-child')!.textContent).toBe('+6.68%');
    expect(rows[1].querySelector('td:last-child')!.textContent).toBe('+7.68%');
    expect(rows[2].querySelector('td:last-child')!.textContent).toBe('+1.00 pp');
    expect(JSON.parse(vi.mocked(apiFetch).mock.calls[0][1]!.body as string).refresh).toBe(false);
    expect(host.querySelector('button')).toBeNull();
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});
