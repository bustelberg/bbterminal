// @vitest-environment happy-dom
import React, { act, StrictMode } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';

const mfa = vi.hoisted(() => ({ listFactors: vi.fn(), enroll: vi.fn(), unenroll: vi.fn(), challengeAndVerify: vi.fn() }));
vi.mock('../../../lib/supabase/client', () => ({ createClient: () => ({ auth: { mfa } }) }));
vi.mock('../../../lib/totp', () => ({ serverSkewSeconds: vi.fn(async () => null), expectedTotp: vi.fn(), explainCode: vi.fn() }));
vi.mock('../../components/account/securityCopy', async () => {
  const actual = await vi.importActual<typeof import('../../components/account/securityCopy')>('../../components/account/securityCopy');
  return { ...actual, useSecurityCopy: () => ({ ...actual.SECURITY_COPY.nl, lang: 'nl' as const }) };
});
import SecurityPage from './page';

let root: Root, host: HTMLDivElement;
beforeEach(() => {
  vi.resetAllMocks();
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  host = document.createElement('div'); document.body.appendChild(host); root = createRoot(host);
  mfa.listFactors.mockResolvedValue({ data: { all: [] }, error: null });
  mfa.enroll.mockResolvedValue({ data: { id: 'pending-factor', totp: { qr_code: '<svg width="219" height="219"></svg>', secret: 'TESTONLYSECRET' } }, error: null });
  mfa.unenroll.mockResolvedValue({ error: null });
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });

it('automatically shows one QR in Strict Mode without a name or start step, and still requires verification', async () => {
  await act(async () => root.render(<StrictMode><SecurityPage /></StrictMode>));
  expect(mfa.enroll).toHaveBeenCalledOnce();
  expect(host.querySelector('svg')).not.toBeNull();
  expect(host.querySelector('#factor-name')).toBeNull();
  expect(host.textContent).not.toContain('Nog geen authenticator');
  expect(host.textContent).not.toContain('De code verandert');
  const confirm = [...host.querySelectorAll('button')].find((button) => button.textContent === 'Inschakelen');
  expect(confirm?.disabled).toBe(true);
  expect(host.querySelector('details')?.open).toBe(false);
  expect(mfa.challengeAndVerify).not.toHaveBeenCalled();
});

it('cleans abandoned factors before creating the replacement QR', async () => {
  mfa.listFactors.mockResolvedValue({ data: { all: [{ id: 'abandoned', factor_type: 'totp', status: 'unverified' }] }, error: null });
  await act(async () => root.render(<SecurityPage />));
  expect(mfa.unenroll).toHaveBeenCalledWith({ factorId: 'abandoned' });
  expect(mfa.unenroll.mock.invocationCallOrder[0]).toBeLessThan(mfa.enroll.mock.invocationCallOrder[0]);
});

it('does not automatically create another authenticator for an already protected account', async () => {
  mfa.listFactors.mockResolvedValue({ data: { all: [{ id: 'active', friendly_name: 'Phone', factor_type: 'totp', status: 'verified', created_at: '2026-01-01' }] }, error: null });
  await act(async () => root.render(<SecurityPage />));
  expect(host.textContent).toContain('Phone');
  expect(mfa.enroll).not.toHaveBeenCalled();
  expect(mfa.unenroll).not.toHaveBeenCalled();
});
