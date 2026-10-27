import { describe, expect, it } from 'vitest';
import { readVerifiedUser, VERIFIED_USER_HEADER, verifiedUserHeaders } from './verifiedUser';

describe('verified layout identity', () => {
  it('replaces a forged administrator header with the validated user', () => {
    const incoming = new Headers({ [VERIFIED_USER_HEADER]: encodeURIComponent(JSON.stringify({ email: 'forged', role: 'admin' })) });
    const forwarded = verifiedUserHeaders(incoming, { email: 'reader@example.com' });
    expect(readVerifiedUser(forwarded.get(VERIFIED_USER_HEADER))).toEqual({ email: 'reader@example.com', role: 'user' });
    expect(incoming.get(VERIFIED_USER_HEADER)).toContain('forged');
  });
  it('removes caller identity on signed-out public requests', () => {
    expect(verifiedUserHeaders(new Headers({ [VERIFIED_USER_HEADER]: 'forged' }), null).has(VERIFIED_USER_HEADER)).toBe(false);
  });
  it('keeps verified roles and unicode email addresses', () => {
    const headers = verifiedUserHeaders(new Headers(), { email: 'tést@example.com', app_metadata: { role: 'admin' } });
    expect(readVerifiedUser(headers.get(VERIFIED_USER_HEADER))).toEqual({ email: 'tést@example.com', role: 'admin' });
  });
  it('fails closed for malformed identity data', () => {
    for (const value of [null, 'invalid', '%XX', '{}', encodeURIComponent('{"email":"x","role":"superuser"}')]) {
      expect(readVerifiedUser(value)).toBeNull();
    }
  });
});
