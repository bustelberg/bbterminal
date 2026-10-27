import { beforeEach, describe, expect, it, vi } from 'vitest';
import { NextRequest } from 'next/server';
import { readVerifiedUser, VERIFIED_USER_HEADER } from './verifiedUser';

const auth = vi.hoisted(() => ({ user: null as null | { email: string; app_metadata: { role: string } }, getUser: vi.fn(), rotate: false, enforce: false,
  levels: { currentLevel: 'aal1', nextLevel: 'aal2' } }));
vi.mock('@supabase/ssr', () => ({
  createServerClient: (_url: unknown, _key: unknown, options: { cookies: { setAll: (cookies: { name: string; value: string; options: object }[]) => void } }) => ({
    auth: { getUser: auth.getUser.mockImplementation(async () => {
      if (auth.rotate) options.cookies.setAll([{ name: 'rotated', value: 'new-value', options: { httpOnly: true } }]);
      return { data: { user: auth.user } };
    }), mfa: { getAuthenticatorAssuranceLevel: async () => ({ data: auth.levels }) } },
  }),
}));
vi.mock('./mfaGate', async importOriginal => ({ ...await importOriginal<typeof import('./mfaGate')>(), mfaEnforced: () => auth.enforce }));
import { proxy } from '../proxy';

beforeEach(() => { auth.user = null; auth.rotate = false; auth.enforce = false; auth.levels = { currentLevel: 'aal1', nextLevel: 'aal2' }; auth.getUser.mockClear(); });

describe('navigation auth handoff', () => {
  it('does exactly one server validation and forwards its identity, preserving refreshed cookies', async () => {
    auth.user = { email: 'reader@example.com', app_metadata: { role: 'user' } };
    auth.rotate = true;
    const response = await proxy(new NextRequest('http://localhost/market', { headers: { [VERIFIED_USER_HEADER]: 'forged-admin' } }));
    expect(auth.getUser).toHaveBeenCalledTimes(1);
    expect(response.status).toBe(200);
    expect(readVerifiedUser(response.headers.get(`x-middleware-request-${VERIFIED_USER_HEADER}`))).toEqual({ email: 'reader@example.com', role: 'user' });
    expect(response.cookies.get('rotated')?.value).toBe('new-value');
    expect(response.headers.get('server-timing')).toContain('auth;dur=');
    expect(response.headers.has(VERIFIED_USER_HEADER)).toBe(false);
  });
  it('does not accept a forged identity for a signed-out visitor', async () => {
    const response = await proxy(new NextRequest('http://localhost/market', { headers: { [VERIFIED_USER_HEADER]: 'forged-admin' } }));
    expect(response.status).toBe(307);
    expect(response.headers.get('location')).toBe('http://localhost/login');
  });
  it('strips forged display identity on public pages', async () => {
    const response = await proxy(new NextRequest('http://localhost/login', { headers: { [VERIFIED_USER_HEADER]: 'forged-admin' } }));
    expect(response.headers.get(`x-middleware-request-${VERIFIED_USER_HEADER}`)).toBeNull();
  });
  it('keeps role authorization on every unprefetched navigation', async () => {
    auth.user = { email: 'reader@example.com', app_metadata: { role: 'user' } };
    const response = await proxy(new NextRequest('http://localhost/users'));
    expect(response.headers.get('location')).toBe('http://localhost/forbidden?from=%2Fusers');
  });
  it('does not forward identity past an unsatisfied MFA gate', async () => {
    auth.enforce = true;
    auth.user = { email: 'reader@example.com', app_metadata: { role: 'admin' } };
    const response = await proxy(new NextRequest('http://localhost/market'));
    expect(response.headers.get('location')).toBe('http://localhost/mfa?next=%2Fmarket');
    expect(response.headers.get(`x-middleware-request-${VERIFIED_USER_HEADER}`)).toBeNull();
  });
  it('still requires MFA enrolment', async () => {
    auth.enforce = true;
    auth.levels.nextLevel = 'aal1';
    auth.user = { email: 'reader@example.com', app_metadata: { role: 'admin' } };
    const response = await proxy(new NextRequest('http://localhost/market'));
    expect(response.headers.get('location')).toBe('http://localhost/account/security');
  });
});
