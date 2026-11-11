// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import CashBalancePanel from './CashBalancePanel';
import { apiFetch } from '../../lib/apiFetch';
import type { AirsComposition } from './AirsCompositionView';

vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../lib/stores/jobs', () => ({
  ExpectedJobFailure: class extends Error {},
  startLocalJob: (_title: string, _kind: string, run: (signal: AbortSignal, report: () => void) => Promise<unknown>) => {
    void run(new AbortController().signal, () => {}).catch(() => {}); return 'job';
  },
}));

it.each(['dummy', 'live', 'changed'] as const)('reads the latest total and balances cash in %s mode', async mode => {
  vi.mocked(apiFetch).mockReset();
  const composition: AirsComposition = { portfolio_id: 1, portfolio: 'Fixed', datum: '2026-11-10', dates: [], rows: [{ id: 1, Fonds: 'Liquiditeiten', Percentage: 10, ISINCode: null, valuta: null, Beleggingscategorie: null, Beleggingssector: null, regio: null, afmCategorie: null }] };
  const balanced = { ...composition, rows: [{ ...composition.rows[0], Percentage: 8.04 }] };
  const leonteq = { success: true, status: 'success', groups: [{ name: 'Cash', weight: '7.32%', positions: [] }] };
  const plan = { composition, total: 101.96, displayed_total: 101.96, current: 10, target: 8.04, row_id: 1, cash_name: 'Liquiditeiten', datum: composition.datum, balanced: false };
  vi.mocked(apiFetch).mockResolvedValueOnce(new Response(JSON.stringify(plan))).mockResolvedValueOnce(
    mode === 'changed' ? new Response(JSON.stringify({ detail: 'The latest AIRS weights changed. Read the total again before executing.' }), { status: 409 })
      : new Response(JSON.stringify({ ...plan, composition: balanced, total: 100, balanced: true })));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host), saved = vi.fn(), busy = vi.fn();
  const click = async (text: string) => act(async () => [...host.querySelectorAll('button')].find(button => button.textContent === text)!.click());
  try {
    await act(async () => root.render(<CashBalancePanel portfolioId={1} composition={composition} leonteq={leonteq} dummy={mode === 'dummy'} disabled={false} onBusyChange={busy} onComposition={saved} />));
    const comparison = () => host.querySelector('[aria-label="Cash comparison"]')!.textContent;
    expect(comparison()).toContain('10.00%');
    expect(comparison()).toContain('7.32%');
    expect(comparison()).toContain('+2.68pp');
    expect(apiFetch).not.toHaveBeenCalled();
    await click('Read latest total');
    expect(host.textContent).toContain('101.96%');
    expect(host.textContent).toContain('10.00% → 8.04%');
    await click('Execute');
    if (mode === 'dummy') {
      expect(apiFetch).toHaveBeenCalledTimes(1);
      expect(saved).not.toHaveBeenCalled();
      expect(host.textContent).toContain('No AIRS changes');
    } else {
      const init = vi.mocked(apiFetch).mock.calls[1][1]!;
      expect(init.method).toBe('POST');
      expect(JSON.parse(String(init.body))).toEqual({ datum: composition.datum, row_id: 1, total: 101.96, current: 10, target: 8.04 });
      expect(saved).toHaveBeenCalledTimes(mode === 'live' ? 1 : 0);
      expect(host.textContent).toContain(mode === 'live' ? 'Cash saved and verified' : 'weights changed');
      if (mode === 'live') {
        expect(comparison()).toContain('8.04%');
        expect(comparison()).toContain('+0.72pp');
      }
    }
    expect(busy).toHaveBeenLastCalledWith(false);
  } finally { await act(async () => root.unmount()); host.remove(); }
});
