/** Request-only identity set by proxy AFTER getUser/MFA/role validation.
 * Never trust a browser-supplied value: proxy replaces this header on every request.
 * This is layout display data, not a replacement for API authorization.
 */
export const VERIFIED_USER_HEADER = 'x-bb-verified-user';
export type VerifiedUser = { email: string; role: 'admin' | 'user' };

export function verifiedUserHeaders(headers: Headers, user: {
  email?: string; app_metadata?: Record<string, unknown>;
} | null): Headers {
  const forwarded = new Headers(headers);
  forwarded.delete(VERIFIED_USER_HEADER);
  if (user?.email) forwarded.set(VERIFIED_USER_HEADER, encodeURIComponent(JSON.stringify({
    email: user.email, role: user.app_metadata?.role === 'admin' ? 'admin' : 'user',
  })));
  return forwarded;
}

export function readVerifiedUser(value: string | null): VerifiedUser | null {
  if (!value) return null;
  try {
    const user = JSON.parse(decodeURIComponent(value));
    return typeof user?.email === 'string' && (user.role === 'admin' || user.role === 'user')
      ? { email: user.email, role: user.role } : null;
  } catch { return null; }
}
