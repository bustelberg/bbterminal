'use client';

import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { useEffect, useRef, useState } from 'react';
import { createClient } from '../../lib/supabase/client';
import { dialog } from '../../lib/dialog';
import { useClickOutside } from '../../lib/hooks/useClickOutside';
import { API_URL } from '../../lib/apiUrl';

type NavItem = { href: string; label: string; userVisible?: true };

// Items marked `userVisible: true` are shown to regular users; everything
// else is admin-only (and to admins viewing as a regular user via the
// "View as user" toggle, only the userVisible items show).
const navItems: NavItem[] = [
  { href: '/', label: 'Welcome', userVisible: true },
  { href: '/earnings', label: 'Earnings Dashboard', userVisible: true },
  { href: '/backtest', label: 'Backtest' },
  { href: '/universe', label: 'Universe Overview' },
  { href: '/longequity-universe', label: 'LongEquity Universe' },
  { href: '/universe_index', label: 'SP500 Universe' },
  { href: '/acwi', label: 'ACWI Universe' },
  { href: '/leonteq', label: 'Leonteq Universe' },
  { href: '/fx-rates', label: 'FX Rates' },
  { href: '/airs-portfolio', label: 'AIRS Portfolio', userVisible: true },
  { href: '/request_gurufocus', label: 'Request GuruFocus' },
  { href: '/benchmarks', label: 'Benchmarks' },
  { href: '/companies', label: 'Companies', userVisible: true },
  { href: '/schedule', label: 'Schedule' },
  { href: '/fees', label: 'Fees' },
  { href: '/api', label: 'API' },
  { href: '/documentation', label: 'Documentation' },
];

const AUTH_PAGES = ['/login', '/set-password'];

function readViewAsCookie(): boolean {
  if (typeof document === 'undefined') return false;
  return document.cookie.split('; ').some((c) => c.startsWith('view_as=user'));
}

function setViewAsCookie(on: boolean) {
  if (typeof document === 'undefined') return;
  if (on) {
    document.cookie = 'view_as=user; path=/; max-age=86400; samesite=lax';
  } else {
    document.cookie = 'view_as=; path=/; max-age=0; samesite=lax';
  }
}

// Multi-session store: every account the browser has authenticated with
// during this session lands here, keyed by email. Lets the user switch
// between any of them instantly via supabase.auth.setSession() — no need
// to retype passwords or re-do the magic-link flow more than once per
// account.
//
// Tokens stay in localStorage on the assumption that a single trusted
// operator (the admin) is on this machine. If you ship to multi-tenant
// browsers, replace with HTTP-only encrypted server-side storage.
const SESSIONS_KEY = 'bbterminal_sessions';

type StoredSession = {
  email: string;
  user_id: string;
  role: 'admin' | 'user';
  access_token: string;
  refresh_token: string;
  // ms-since-epoch of last refresh. Mostly for debugging / staleness.
  saved_at: number;
};

function readSessions(): StoredSession[] {
  if (typeof window === 'undefined') return [];
  try {
    const raw = localStorage.getItem(SESSIONS_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? (parsed as StoredSession[]) : [];
  } catch {
    return [];
  }
}

function writeSessions(s: StoredSession[]) {
  if (typeof window === 'undefined') return;
  localStorage.setItem(SESSIONS_KEY, JSON.stringify(s));
}

function upsertSession(entry: StoredSession): StoredSession[] {
  const list = readSessions().filter((s) => s.email !== entry.email);
  list.push(entry);
  writeSessions(list);
  return list;
}

function removeSessionByEmail(email: string): StoredSession[] {
  const list = readSessions().filter((s) => s.email !== email);
  writeSessions(list);
  return list;
}

function clearAllSessions() {
  if (typeof window === 'undefined') return;
  localStorage.removeItem(SESSIONS_KEY);
}

type SwitchableUser = { id: string; email: string | null; role: 'admin' | 'user' };

// `initialUser` comes from the root layout's server-side getUser() call,
// which has already been validated by proxy.ts. Passing it in lets the
// sidebar render on first paint instead of waiting for the client-side
// getUser() — and avoids the "tab-duplication race" where two tabs both
// try to refresh tokens and the loser sees a transient null user, which
// previously made the sidebar disappear until a hard refresh.
type Props = {
  initialUser: { email: string; role: 'admin' | 'user' } | null;
};

export default function Sidebar({ initialUser }: Props) {
  const pathname = usePathname();
  const router = useRouter();
  const [email, setEmail] = useState<string | null>(initialUser?.email ?? null);
  const [role, setRole] = useState<string | null>(initialUser?.role ?? null);
  // `checked` gates the "show nothing" guard for unauthenticated users.
  // When server already saw a user, we're done checking; when it didn't
  // (auth pages, logged-out state), defer to client-side resolution.
  const [checked, setChecked] = useState(initialUser != null);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [viewAsUser, setViewAsUser] = useState(false);

  // Account switcher state
  const [accountMenuOpen, setAccountMenuOpen] = useState(false);
  const accountMenuRef = useRef<HTMLDivElement>(null);
  const [otherUsers, setOtherUsers] = useState<SwitchableUser[]>([]);
  const [storedSessions, setStoredSessions] = useState<StoredSession[]>([]);
  const [switching, setSwitching] = useState(false);

  useEffect(() => {
    const supabase = createClient();

    // `event` is null on initial mount; on auth-state-change callbacks
    // we get the actual event ('SIGNED_OUT', 'TOKEN_REFRESHED', etc.).
    // We use it to decide whether a null `getUser()` should clear the
    // sidebar or be ignored as a transient race.
    async function refresh(event: string | null = null) {
      const [userRes, sessionRes] = await Promise.all([
        supabase.auth.getUser(),
        supabase.auth.getSession(),
      ]);
      const user = userRes.data.user;
      const session = sessionRes.data.session;

      if (user?.email) {
        // Got a real user — adopt it as the live state.
        setEmail(user.email);
        const meta = (user.app_metadata ?? {}) as { role?: string };
        const detectedRole: 'admin' | 'user' = meta.role === 'admin' ? 'admin' : 'user';
        setRole(detectedRole);
        if (user.id && session) {
          const updated = upsertSession({
            email: user.email,
            user_id: user.id,
            role: detectedRole,
            access_token: session.access_token,
            refresh_token: session.refresh_token,
            saved_at: Date.now(),
          });
          setStoredSessions(updated);
        }
      } else if (event === 'SIGNED_OUT') {
        // Explicit sign-out — clear the sidebar. This is the ONE case
        // where a null user should blank the UI.
        setEmail(null);
        setRole(null);
      } else if (initialUser == null) {
        // No initial server-side user AND client also sees none — show
        // the unauthenticated state (no sidebar). This is the standard
        // "logged out" path on auth pages or first visit.
        setEmail(null);
        setRole(null);
      }
      // Otherwise: a transient null on initial mount or a TOKEN_REFRESHED
      // event from a concurrent tab. Leave the sidebar as-is; the next
      // auth-state-change should resolve it. This is the fix for the
      // "duplicate tab → sidebar disappears" race.

      setChecked(true);
      if (!user?.email) setStoredSessions(readSessions());
    }

    refresh();

    // The Sidebar lives in the root layout and stays mounted across
    // /login ↔ /. Without this subscription, role/email would be frozen
    // at whoever signed in *first* — switching accounts would leave the
    // sidebar showing the previous user's nav items (or nothing if the
    // new account's role hasn't been picked up yet).
    const { data: { subscription } } = supabase.auth.onAuthStateChange((event) => {
      refresh(event);
    });

    setViewAsUser(readViewAsCookie());
    return () => subscription.unsubscribe();
  }, [initialUser]);

  useClickOutside(accountMenuRef, () => setAccountMenuOpen(false), accountMenuOpen);

  // When the admin opens the menu, fetch the user list once so we know who
  // we can switch to. Skipped for non-admins (they can't list users) and
  // when impersonating (the menu only shows "Switch back" in that case).
  useEffect(() => {
    if (!accountMenuOpen) return;
    if (role !== 'admin') return;
    if (otherUsers.length > 0) return;
    let cancelled = false;
    (async () => {
      const supabase = createClient();
      const { data: { session } } = await supabase.auth.getSession();
      if (!session) return;
      const r = await fetch(`${API_URL}/api/auth/users`, {
        headers: { Authorization: `Bearer ${session.access_token}` },
      });
      if (!r.ok) return;
      const data = await r.json();
      if (cancelled) return;
      // Exclude the currently signed-in admin.
      const list: SwitchableUser[] = (data.users ?? []).filter(
        (u: SwitchableUser) => u.email && u.email !== email,
      );
      setOtherUsers(list);
    })();
    return () => { cancelled = true; };
  }, [accountMenuOpen, role, email, otherUsers.length]);

  function toggleViewAs() {
    const next = !viewAsUser;
    setViewAsCookie(next);
    setViewAsUser(next);
    router.refresh();
    if (next) router.push('/');
  }

  /** First-time sign-in for an account that isn't yet in the multi-session
   * store. Backend mints fresh `{access_token, refresh_token}` for the
   * target via the admin magic-link → verify_otp dance; we set them
   * client-side, store them, and reload. No URL fragment, no redirect
   * dance through Supabase's verify endpoint. */
  async function switchToNewUser(target: SwitchableUser) {
    if (!target.id || switching) return;
    setSwitching(true);
    try {
      const supabase = createClient();
      const { data: { session } } = await supabase.auth.getSession();
      if (!session) {
        await dialog.alert('Not signed in.');
        return;
      }
      const r = await fetch(`${API_URL}/api/auth/impersonate`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${session.access_token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ target_user_id: target.id }),
      });
      if (!r.ok) {
        const body = await r.text();
        await dialog.alert(`Switch failed:\n${r.status}: ${body}`);
        return;
      }
      const data = await r.json();
      if (!data.access_token || !data.refresh_token) {
        await dialog.alert('Server returned no session tokens');
        return;
      }
      const { error } = await supabase.auth.setSession({
        access_token: data.access_token,
        refresh_token: data.refresh_token,
      });
      if (error) {
        await dialog.alert(`Could not establish session: ${error.message}`);
        return;
      }
      // Pre-populate the multi-session store with the new account so it
      // shows up in the dropdown right after the reload (the mount-time
      // capture would do it anyway, but storing here too means we never
      // see the "first time" entry for this account again).
      upsertSession({
        email: target.email ?? '',
        user_id: target.id,
        role: target.role,
        access_token: data.access_token,
        refresh_token: data.refresh_token,
        saved_at: Date.now(),
      });
      setViewAsCookie(false);
      // Hard reload so the middleware reads the new cookies for the
      // first server-rendered request.
      window.location.href = '/';
    } catch (e) {
      await dialog.alert(`Switch failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setSwitching(false);
    }
  }

  /** Switch instantly to a previously-stored account using its tokens.
   * No magic link, no password — just supabase.auth.setSession() and a
   * hard reload so the middleware re-reads the new cookies on the
   * subsequent request. */
  async function switchToStoredSession(target: StoredSession) {
    if (switching) return;
    if (target.email === email) return;
    setSwitching(true);
    try {
      const supabase = createClient();
      const { error } = await supabase.auth.setSession({
        access_token: target.access_token,
        refresh_token: target.refresh_token,
      });
      if (error) {
        // Tokens have expired or been revoked — drop the entry from the
        // store and tell the user to re-authenticate via the user list.
        const remaining = removeSessionByEmail(target.email);
        setStoredSessions(remaining);
        await dialog.alert(
          `Could not restore ${target.email}: ${error.message}\n\n` +
          `The stored session is no longer valid. Use the user list to sign in again.`,
        );
        return;
      }
      // Hard reload so the middleware sees the new cookies on the next
      // request. router.push wouldn't propagate the cookie change to
      // the server in time and the middleware would still see the old
      // user, sometimes redirecting to /forbidden in transit.
      setViewAsCookie(false);
      window.location.href = '/';
    } catch (e) {
      await dialog.alert(`Switch failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setSwitching(false);
    }
  }

  async function handleSignOut() {
    const supabase = createClient();
    await supabase.auth.signOut();
    // Wipe the multi-session store so the next login starts clean —
    // any stored tokens for other accounts may be revoked alongside
    // the current sign-out, and we don't want to surface dead entries.
    clearAllSessions();
    setStoredSessions([]);
    setViewAsCookie(false);
    router.push('/login');
    router.refresh();
  }

  async function handleDeleteAccount() {
    setDeleting(true);
    try {
      const supabase = createClient();
      const { data: { session } } = await supabase.auth.getSession();
      if (!session) throw new Error('No session');
      const res = await fetch(`${API_URL}/api/auth/delete-account`, {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${session.access_token}` },
      });
      if (!res.ok) {
        const body = await res.text();
        throw new Error(`${res.status}: ${body}`);
      }
      await supabase.auth.signOut();
      router.push('/login');
      router.refresh();
    } catch (err) {
      dialog.alert(`Failed to delete account:\n${err instanceof Error ? err.message : err}`, { title: 'Account deletion failed' });
    } finally {
      setDeleting(false);
      setShowDeleteConfirm(false);
    }
  }

  // Hide sidebar on auth pages always; otherwise wait for the auth check to
  // finish before deciding whether to render. Without this, a flicker of
  // ambiguous state can cause the sidebar to disappear right after a
  // successful sign-in / impersonation while `getUser()` is still resolving.
  if (AUTH_PAGES.includes(pathname)) return null;
  if (!checked) return null;
  if (!email) return null;

  const isAdmin = role === 'admin';
  const effectiveRole: 'admin' | 'user' = isAdmin && !viewAsUser ? 'admin' : 'user';
  const visibleNav = effectiveRole === 'admin'
    ? navItems
    : navItems.filter((n) => n.userVisible);

  return (
    <aside className="w-56 shrink-0 border-r border-gray-800/60 bg-[#0b0d13] flex flex-col">
      <div className="px-5 py-5 border-b border-gray-800/60">
        <Link href="/" className="text-lg font-semibold tracking-tight text-white hover:text-gray-300 transition-colors">
          BBTerminal
        </Link>
      </div>
      <nav className="flex-1 min-h-0 overflow-y-auto p-3 space-y-1">
        {visibleNav.map(({ href, label }) => (
          <Link
            key={href}
            href={href}
            className={`block px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
              pathname === href || (pathname.startsWith(href + '/') && href !== '/')
                ? 'bg-indigo-600/15 text-indigo-400'
                : 'text-gray-400 hover:text-white hover:bg-white/5'
            }`}
          >
            {label}
          </Link>
        ))}
        {effectiveRole === 'admin' && (
          <Link
            href="/users"
            className={`block px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
              pathname === '/users'
                ? 'bg-indigo-600/15 text-indigo-400'
                : 'text-gray-400 hover:text-white hover:bg-white/5'
            }`}
          >
            Users
          </Link>
        )}
      </nav>
      {isAdmin && (
        <div className="px-3 py-2 border-t border-gray-800/60">
          <label
            className="flex items-center gap-2 px-3 py-2 rounded-lg cursor-pointer hover:bg-white/5"
            title="Sidebar + middleware switch to the regular-user view, so you can verify exactly what non-admins see."
          >
            <input
              type="checkbox"
              checked={viewAsUser}
              onChange={toggleViewAs}
              className="accent-amber-500 w-4 h-4 cursor-pointer"
            />
            <span className={`text-xs ${viewAsUser ? 'text-amber-400 font-medium' : 'text-gray-400'}`}>
              View as regular user
            </span>
          </label>
        </div>
      )}
      <div className="p-3 border-t border-gray-800/60 space-y-1">
        {email && (() => {
          // Sessions other than the one we're currently signed in as.
          const otherStored = storedSessions.filter((s) => s.email !== email);
          // DB users (admin only) that haven't been stored yet — these
          // need the magic-link impersonation flow on first switch.
          const storedEmails = new Set(storedSessions.map((s) => s.email));
          const newUsers = otherUsers.filter((u) => u.email && !storedEmails.has(u.email));
          const isImpersonating = otherStored.length > 0 && role === 'user';
          return (
            <div className="relative" ref={accountMenuRef}>
              <button
                type="button"
                onClick={() => setAccountMenuOpen((o) => !o)}
                className="w-full px-3 py-1.5 rounded-lg text-left flex items-center gap-2 hover:bg-white/5 transition-colors"
                title="Switch account"
              >
                <span className="flex-1 min-w-0 truncate text-sm text-gray-300" title={email}>
                  {email}
                </span>
                {isImpersonating ? (
                  <span className="text-[9px] uppercase tracking-wider text-amber-400 shrink-0" title="Switch back from the menu">
                    impersonating
                  </span>
                ) : role === 'admin' ? (
                  <span className="text-[9px] uppercase tracking-wider text-indigo-400 shrink-0">admin</span>
                ) : null}
                <svg className="w-3 h-3 text-gray-500 shrink-0" viewBox="0 0 20 20" fill="currentColor">
                  <path fillRule="evenodd" d="M5.23 7.21a.75.75 0 011.06.02L10 11.168l3.71-3.938a.75.75 0 111.08 1.04l-4.25 4.5a.75.75 0 01-1.08 0l-4.25-4.5a.75.75 0 01.02-1.06z" clipRule="evenodd" />
                </svg>
              </button>

              {accountMenuOpen && (
                <div className="absolute bottom-full left-0 right-0 mb-1 bg-[#1a1d27] border border-gray-700 rounded-lg shadow-xl overflow-hidden">
                  <div className="px-3 py-2 border-b border-gray-800/60">
                    <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-0.5">Signed in as</div>
                    <div className="text-xs text-gray-200 font-mono truncate">{email}</div>
                  </div>

                  {/* Stored sessions — instant switch via setSession() */}
                  {otherStored.length > 0 && (
                    <div>
                      <div className="px-3 pt-2 pb-1 text-[10px] uppercase tracking-wider text-gray-500 border-t border-gray-800/60">
                        Switch to (instant)
                      </div>
                      {otherStored.map((s) => (
                        <button
                          key={s.email}
                          onClick={() => switchToStoredSession(s)}
                          disabled={switching}
                          className="w-full px-3 py-2 text-left hover:bg-white/[0.04] transition-colors disabled:opacity-50 flex items-center gap-2"
                        >
                          <div className="flex-1 min-w-0">
                            <div className="text-xs text-gray-200 truncate">{s.email}</div>
                          </div>
                          <span
                            className={`text-[9px] uppercase tracking-wider shrink-0 ${
                              s.role === 'admin' ? 'text-indigo-400' : 'text-gray-500'
                            }`}
                          >
                            {s.role}
                          </span>
                        </button>
                      ))}
                    </div>
                  )}

                  {/* New (not-yet-stored) DB users — admin only. First click
                      triggers the magic-link sign-in; the new session lands
                      in the multi-session store automatically. */}
                  {role === 'admin' && newUsers.length > 0 && (
                    <div>
                      <div className="px-3 pt-2 pb-1 text-[10px] uppercase tracking-wider text-gray-500 border-t border-gray-800/60">
                        Sign in as (first time)
                      </div>
                      {newUsers.map((u) => (
                        <button
                          key={u.id}
                          onClick={() => switchToNewUser(u)}
                          disabled={switching}
                          className="w-full px-3 py-2 text-left hover:bg-white/[0.04] transition-colors disabled:opacity-50 flex items-center gap-2"
                        >
                          <div className="flex-1 min-w-0">
                            <div className="text-xs text-gray-200 truncate">{u.email}</div>
                          </div>
                          <span
                            className={`text-[9px] uppercase tracking-wider shrink-0 ${
                              u.role === 'admin' ? 'text-indigo-400' : 'text-gray-500'
                            }`}
                          >
                            {u.role}
                          </span>
                        </button>
                      ))}
                    </div>
                  )}

                  {/* Empty-state: admin with no other accounts at all. */}
                  {role === 'admin' && otherStored.length === 0 && newUsers.length === 0 && (
                    <div className="px-3 py-2 text-[11px] text-gray-500 border-t border-gray-800/60">
                      No other accounts — add one in{' '}
                      <Link href="/users" className="text-indigo-400 hover:underline" onClick={() => setAccountMenuOpen(false)}>
                        Users
                      </Link>.
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })()}
        <button
          onClick={handleSignOut}
          className="w-full px-3 py-2.5 rounded-lg text-sm font-medium text-gray-500 hover:text-white hover:bg-white/5 transition-colors text-left"
        >
          Sign out
        </button>
        {!showDeleteConfirm ? (
          <button
            onClick={() => setShowDeleteConfirm(true)}
            className="w-full px-3 py-2.5 rounded-lg text-sm font-medium text-gray-500 hover:text-rose-400 hover:bg-rose-500/10 transition-colors text-left"
          >
            Delete account
          </button>
        ) : (
          <div className="px-3 py-2 space-y-2">
            <p className="text-sm text-rose-400">Are you sure? This cannot be undone.</p>
            <div className="flex gap-2">
              <button
                onClick={handleDeleteAccount}
                disabled={deleting}
                className="flex-1 px-2 py-1.5 rounded-lg text-sm font-medium bg-rose-600 hover:bg-rose-500 text-white transition-colors disabled:opacity-50"
              >
                {deleting ? 'Deleting...' : 'Yes, delete'}
              </button>
              <button
                onClick={() => setShowDeleteConfirm(false)}
                className="flex-1 px-2 py-1.5 rounded-lg text-sm font-medium text-gray-400 hover:text-white hover:bg-white/5 transition-colors"
              >
                Cancel
              </button>
            </div>
          </div>
        )}
      </div>
    </aside>
  );
}
