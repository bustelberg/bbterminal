'use client';

import Image from 'next/image';
import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import { createClient } from '../../lib/supabase/client';
import { dialog } from '../../lib/dialog';
import { API_URL } from '../../lib/apiUrl';
import { apiFetch } from '../../lib/apiFetch';
import { isUserAllowedPath } from '../../lib/userAllowedPaths';
import { useSidebarCopy, type NavKey } from './sidebarCopy';
import LangSwitch from './LangSwitch';
import { claimLangFor, useLang } from '../../lib/i18n';
import { purgeLegacySessions } from '../../lib/purgeLegacySessions';

// ⚠⚠ NO `label` HERE — the name of a page is COPY and lives in `sidebarCopy.ts`, keyed by href.
// This file owns the ORDER, the sections and the visibility rules, none of which is a language.
// `NavKey` is what ties the two together: a nav entry whose href has no label will not compile.
type NavItem = { href: NavKey };
// A collapsible group: its `href` makes the header itself a link; `children` are indented
// sub-pages shown when expanded.
type NavSection = { href: NavKey; children: NavItem[] };
type NavEntry = NavItem | NavSection;

const isSection = (e: NavEntry): e is NavSection => 'children' in e;

// Which items a regular user (or an admin in "view as user" mode) sees is
// derived from `isUserAllowedPath` — the SAME allow-list the route gate
// (`proxy.ts`) enforces — so the nav can never show a page the user can't open.
const navItems: NavEntry[] = [
  { href: '/' },
  // The AIRS books, their models and the analysis built on them.
  { href: '/management-dashboard' },
  // ⚠ BESIDE THE DASHBOARD. It is a reading tool rather than a pipeline tool: two companies'
  // fundamentals side by side, off the same view the Fundamental button opens. Filed with the
  // one-off admin tools near the bottom it would read as one of them.
  { href: '/research-dashboard' },
  // ⚠ NEAR THE TOP, NOT NEAR THE BOTTOM (moved up 2026-08-13; Research Dashboard came in above it
  // on 2026-08-19, so it is no longer literally second). It is the page that answers "is the data
  // behind everything else current" — the scheduled strategies, the pipeline, and now every
  // automatic job — so it is checked FIRST when a number looks wrong. Fifteen entries down, beside
  // the one-off tools, it read as one of them.
  { href: '/schedule' },
  { href: '/earnings' },
  { href: '/backtest' },
  { href: '/diversifier' },
  {
    href: '/universe',
    children: [
      { href: '/longequity-universe' },
      { href: '/sp500' },
      { href: '/acwi' },
      { href: '/leonteq' },
    ],
  },
  { href: '/fx-rates' },
  { href: '/timezone' },
  { href: '/airs-portfolio' },
  { href: '/request_gurufocus' },
  { href: '/benchmarks' },
  { href: '/isin-compare' },
  { href: '/asset-pipeline' },
  { href: '/alphalab' },
  { href: '/signal-lab' },
  { href: '/fees' },
  { href: '/api' },
  { href: '/network' },
];


// ⚠ `/auth/confirm` BELONGS HERE AND WAS MISSING. It is a signed-out page like the other two —
// the whole point is that nobody is authenticated on it yet — so the rail rendered beside it,
// which on a phone meant the mobile top bar sat above a card whose only control is one button.
const AUTH_PAGES = ['/login', '/set-password', '/auth/confirm', '/mfa'];

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
  // ⚠ THE SHARED PREFERENCE, NOT A LOCAL ONE — `useLang` is an external store, so this switch
  // and any open modal read the same value and move together. See `lib/i18n`.
  const [lang, setLang] = useLang();
  // The sidebar's own chrome — the account block at the foot and this switch's own label. Reads
  // the same preference `lang` above does, so the control and the words around it cannot disagree.
  const t = useSidebarCopy();
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
  // Which collapsible nav sections are expanded. Universe Overview starts open.
  // ⚠ KEYED BY HREF, NOT BY LABEL. The label is now translated, so a language flip would change
  // the key and silently collapse (or re-open) whatever the reader had set. An href does not move.
  const [openSections, setOpenSections] = useState<Set<string>>(() => new Set(['/universe']));
  // Mobile nav: off-canvas drawer on < lg, static rail on lg+.
  const [drawerOpen, setDrawerOpen] = useState(false);


  /**
   * ⚠⚠ THE LANGUAGE IS CLAIMED FROM THE SERVER-RESOLVED IDENTITY, BEFORE ANY NETWORK CALL. The
   * claim below in `refresh()` sits behind `await supabase.auth.getUser()`, and on the first load
   * after this rule shipped that meant: paint the whole app in the previous reader's language,
   * wait out a Supabase round trip (100 ms to a couple of SECONDS locally), then repaint every
   * label in Dutch. Reported as "something changes a couple of seconds after load" — and the thing
   * changing was every string on the page, mid-read.
   *
   * `initialUser` comes from the root layout, which already resolved the session server-side, so
   * it is known at FIRST RENDER. Claiming here collapses the repaint into hydration, which is the
   * corrected paint `lib/i18n.ts` already documents and accepts.
   *
   * ⚠ NON-NULL ONLY. A null `initialUser` is not proof of being signed out — it is also the
   * transient race `refresh()` exists to absorb — and claiming for `null` here would wipe a live
   * reader's choice on any load where the server cookie read lost that race. The signed-out claim
   * stays in the two branches below that actually know.
   */
  useEffect(() => {
    if (initialUser?.email) claimLangFor(initialUser.email);
  }, [initialUser]);

  /**
   * ⚠ THE RETIRED SWITCHER'S REFRESH TOKENS, CLEARED ON EVERY LOAD. Deleting the feature left
   * them sitting in `localStorage` in every browser that had used it — valid, invisible, and
   * reachable by anything on the origin. The Sidebar mounts in the root layout, so this is the
   * one place that runs on every authenticated page. See `lib/purgeLegacySessions`.
   *
   * ⚠ NOT GATED ON THE USER. It has nothing to do with who is signed in, and the browsers most
   * likely to be carrying stale tokens are the ones where nobody is.
   */
  useEffect(() => { purgeLegacySessions(); }, []);

  useEffect(() => {
    const supabase = createClient();

    // `event` is null on initial mount; on auth-state-change callbacks
    // we get the actual event ('SIGNED_OUT', 'TOKEN_REFRESHED', etc.).
    // We use it to decide whether a null `getUser()` should clear the
    // sidebar or be ignored as a transient race.
    async function refresh(event: string | null = null) {
      // ⚠ `getUser()` ALONE NOW. The parallel `getSession()` existed only to hand raw tokens to
      // the account switcher's store; with that gone, the sidebar needs an identity and never the
      // credentials behind it — so this no longer reads them at all.
      const { data: { user } } = await supabase.auth.getUser();

      if (user?.email) {
        // Got a real user — adopt it as the live state.
        setEmail(user.email);
        // ⚠⚠ THE LANGUAGE FOLLOWS THE ACCOUNT, NOT THE BROWSER PROFILE. `bb:lang` defaults to `nl`
        // only when NOTHING is stored, and the only thing that ever stores it is somebody pressing
        // the switch — so a new user signing up on a machine where an earlier account pressed EN
        // read English, having chosen nothing. ⚠ CALLED ONLY IN THE THREE BRANCHES THAT KNOW THE
        // ANSWER, never on the transient-null fall-through below: claiming for `null` there would
        // wipe a live reader's choice on every duplicate-tab token refresh.
        claimLangFor(user.email);
        const meta = (user.app_metadata ?? {}) as { role?: string };
        const detectedRole: 'admin' | 'user' = meta.role === 'admin' ? 'admin' : 'user';
        setRole(detectedRole);
      } else if (event === 'SIGNED_OUT') {
        // Explicit sign-out — clear the sidebar. This is the ONE case
        // where a null user should blank the UI.
        setEmail(null);
        setRole(null);
        claimLangFor(null);
      } else if (initialUser == null) {
        // No initial server-side user AND client also sees none — show
        // the unauthenticated state (no sidebar). This is the standard
        // "logged out" path on auth pages or first visit.
        setEmail(null);
        setRole(null);
        claimLangFor(null);
      }
      // Otherwise: a transient null on initial mount or a TOKEN_REFRESHED
      // event from a concurrent tab. Leave the sidebar as-is; the next
      // auth-state-change should resolve it. This is the fix for the
      // "duplicate tab → sidebar disappears" race.

      setChecked(true);
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

  // Close the mobile drawer on every route change so tapping a nav link
  // doesn't leave the overlay covering the page.
  useEffect(() => {
    setDrawerOpen(false);
  }, [pathname]);

  function toggleViewAs() {
    const next = !viewAsUser;
    setViewAsCookie(next);
    setViewAsUser(next);
    router.refresh();
    if (next) router.push('/');
  }

  async function handleSignOut() {
    const supabase = createClient();
    await supabase.auth.signOut();
    // ⚠ Belt and braces with the mount-time purge: signing out is the one moment somebody is
    // deliberately leaving the browser, so it is the last chance to take the retired switcher's
    // refresh tokens with them.
    purgeLegacySessions();
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
      const res = await apiFetch(`${API_URL}/api/auth/delete-account`, {
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
  // successful sign-in while `getUser()` is still resolving.
  if (AUTH_PAGES.includes(pathname)) return null;
  if (!checked) return null;
  if (!email) return null;

  const isAdmin = role === 'admin';
  const effectiveRole: 'admin' | 'user' = isAdmin && !viewAsUser ? 'admin' : 'user';
  const isAdminView = effectiveRole === 'admin';
  // Filter top-level items + section children by visibility; drop sections that
  // end up empty (and aren't a link in their own right).
  const visibleNav: NavEntry[] = navItems
    .map((e): NavEntry | null => {
      if (isSection(e)) {
        const children = e.children.filter((c) => isAdminView || isUserAllowedPath(c.href));
        const sectionAllowed = isAdminView
          || (e.href ? isUserAllowedPath(e.href) : false)
          || children.length > 0;
        if (!sectionAllowed) return null;
        if (children.length === 0 && !e.href) return null;
        return { ...e, children };
      }
      return isAdminView || isUserAllowedPath(e.href) ? e : null;
    })
    .filter((e): e is NavEntry => e !== null);

  const isActive = (href: string) =>
    pathname === href || (pathname.startsWith(href + '/') && href !== '/');
  const toggleSection = (key: string) =>
    setOpenSections((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });

  return (
    <>
      {/* Mobile top bar (< lg). In normal flow — the body is a column on
          mobile — so it pushes content down; hidden on lg+ where the static
          rail takes over. Holds the hamburger that opens the drawer. */}
      <div className="lg:hidden sticky top-0 z-30 flex items-center gap-3 h-14 px-4 border-b border-neutral-800/60 bg-sidebar">
        <button
          type="button"
          onClick={() => setDrawerOpen(true)}
          aria-label="Open navigation menu"
          aria-expanded={drawerOpen}
          className="p-2 -ml-2 rounded-lg text-fg-muted hover:text-fg-strong hover:bg-overlay/5 transition-colors"
        >
          <svg className="w-5 h-5" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.8">
            <path strokeLinecap="round" d="M3 5h14M3 10h14M3 15h14" />
          </svg>
        </button>
        <Link href="/" className="flex items-center gap-2">
          <Image src="/logo.jpg" alt="BBTerminal" width={24} height={24} className="rounded-md shrink-0" priority />
          <span className="text-base font-semibold tracking-tight text-fg-strong">BBTerminal</span>
        </Link>
      </div>

      {/* Scrim behind the open drawer (mobile only). Tap to close. */}
      {drawerOpen && (
        <div
          className="lg:hidden fixed inset-0 z-40 bg-scrim/60"
          onClick={() => setDrawerOpen(false)}
          aria-hidden
        />
      )}

      <aside
        className={`bg-sidebar flex flex-col border-r border-neutral-800/60
          fixed inset-y-0 left-0 z-50 w-64 max-w-[82vw] transition-transform duration-200 ease-out
          ${drawerOpen ? 'translate-x-0' : '-translate-x-full'}
          lg:static lg:z-auto lg:w-56 lg:max-w-none lg:translate-x-0 lg:shrink-0`}
      >
      <div className="px-5 py-5 border-b border-neutral-800/60 flex items-center justify-between">
        <Link href="/" className="flex items-center gap-2.5 group">
          <Image
            src="/logo.jpg"
            alt="BBTerminal"
            width={28}
            height={28}
            className="rounded-md shrink-0"
            priority
          />
          <span className="text-lg font-semibold tracking-tight text-fg-strong group-hover:text-fg-soft transition-colors">
            BBTerminal
          </span>
        </Link>
        {/* Close button — drawer only (mobile). */}
        <button
          type="button"
          onClick={() => setDrawerOpen(false)}
          aria-label="Close navigation menu"
          className="lg:hidden p-1.5 -mr-1.5 rounded-lg text-fg-muted hover:text-fg-strong hover:bg-overlay/5 transition-colors"
        >
          <svg className="w-5 h-5" viewBox="0 0 20 20" fill="currentColor">
            <path d="M6.28 5.22a.75.75 0 00-1.06 1.06L8.94 10l-3.72 3.72a.75.75 0 101.06 1.06L10 11.06l3.72 3.72a.75.75 0 101.06-1.06L11.06 10l3.72-3.72a.75.75 0 00-1.06-1.06L10 8.94 6.28 5.22z" />
          </svg>
        </button>
      </div>
      <nav className="flex-1 min-h-0 overflow-y-auto p-3 space-y-1">
        {visibleNav.map((entry) => {
          if (!isSection(entry)) {
            return (
              <Link
                key={entry.href}
                href={entry.href}
                className={`block px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                  isActive(entry.href)
                    ? 'bg-accent-600/15 text-accent-400'
                    : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/5'
                }`}
              >
                {t.nav[entry.href]}
              </Link>
            );
          }
          const headerActive = entry.href ? isActive(entry.href) : false;
          // Keep a section open whenever its header or one of its children is
          // the active route, so the current page is never hidden behind a
          // collapsed group.
          const open =
            openSections.has(entry.href) ||
            headerActive ||
            entry.children.some((c) => isActive(c.href));
          return (
            <div key={entry.href}>
              <div className={`flex items-center rounded-lg ${headerActive ? 'bg-accent-600/15' : 'hover:bg-overlay/5'}`}>
                {entry.href ? (
                  <Link
                    href={entry.href}
                    className={`flex-1 px-3 py-2.5 rounded-l-lg text-sm font-medium transition-colors ${
                      headerActive ? 'text-accent-400' : 'text-fg-muted hover:text-fg-strong'
                    }`}
                  >
                    {t.nav[entry.href]}
                  </Link>
                ) : (
                  <span className="flex-1 px-3 py-2.5 text-sm font-medium text-fg-muted">{t.nav[entry.href]}</span>
                )}
                <button
                  type="button"
                  onClick={() => toggleSection(entry.href)}
                  aria-label={open ? t.collapseSection(t.nav[entry.href]) : t.expandSection(t.nav[entry.href])}
                  className="px-2 py-2.5 text-fg-subtle hover:text-fg-strong transition-colors"
                >
                  <svg className={`w-3 h-3 transition-transform ${open ? 'rotate-90' : ''}`} viewBox="0 0 20 20" fill="currentColor">
                    <path fillRule="evenodd" d="M7.21 5.23a.75.75 0 011.06.02l4.5 4.25a.75.75 0 010 1.08l-4.5 4.25a.75.75 0 11-1.04-1.08L11.168 10 7.23 6.29a.75.75 0 01-.02-1.06z" clipRule="evenodd" />
                  </svg>
                </button>
              </div>
              {open && (
                <div className="mt-1 ml-4 pl-2 border-l border-neutral-800/60 space-y-1">
                  {entry.children.map((c) => (
                    <Link
                      key={c.href}
                      href={c.href}
                      className={`block px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                        isActive(c.href)
                          ? 'bg-accent-600/15 text-accent-400'
                          : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/5'
                      }`}
                    >
                      {t.nav[c.href]}
                    </Link>
                  ))}
                </div>
              )}
            </div>
          );
        })}
        {effectiveRole === 'admin' && (
          <Link
            href="/users"
            className={`block px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
              pathname === '/users'
                ? 'bg-accent-600/15 text-accent-400'
                : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/5'
            }`}
          >
            Users
          </Link>
        )}
      </nav>
      {isAdmin && (
        <div className="px-3 py-2 border-t border-neutral-800/60">
          <label
            className="flex items-center gap-2 px-3 py-2 rounded-lg cursor-pointer hover:bg-overlay/5"
            title="Sidebar + middleware switch to the regular-user view, so you can verify exactly what non-admins see."
          >
            <input
              type="checkbox"
              checked={viewAsUser}
              onChange={toggleViewAs}
              className="accent-warn-500 w-4 h-4 cursor-pointer"
            />
            <span className={`text-xs ${viewAsUser ? 'text-warn-400 font-medium' : 'text-fg-muted'}`}>
              View as regular user
            </span>
          </label>
        </div>
      )}
      <div className="p-3 border-t border-neutral-800/60 space-y-1">
        {/* ⚠ THE ACCOUNT SWITCHER IS GONE (2026-09-08, on request) — the whole impersonation
            feature went with `POST /api/auth/impersonate`, and this menu was its only entry point.
            What is left is the one thing it also did: say who you are signed in as. Promotion and
            demotion live on /users; previewing the non-admin UI is the "View as regular user"
            toggle above, which changes nothing about the session. */}
        {email && (
          <div className="w-full px-3 py-1.5 flex items-center gap-2">
            <span className="flex-1 min-w-0 truncate text-sm text-fg-soft" title={email}>
              {email}
            </span>
            {role === 'admin' && (
              <span className="text-[10px] uppercase tracking-wider text-accent-400 shrink-0">
                admin
              </span>
            )}
          </div>
        )}
        {/* ⚠ IN THE ACCOUNT BLOCK, NOT IN `navItems`. It is a property of the READER rather than
            a page of the app — same argument as the language switch below it — and the main nav is
            ordered by what the terminal DOES. It also keeps `NavKey` for pages that appear there. */}
        {email && (
          <Link
            href="/account/security"
            className="block w-full px-3 py-2.5 rounded-lg text-sm font-medium text-fg-subtle
                       hover:text-fg-strong hover:bg-overlay/5 transition-colors"
          >
            {t.security}
          </Link>
        )}
        {/* ⚠⚠ GLOBAL, AND THAT IS A DELIBERATE TRADE (2026-08-21, on request). It sets ONE
            stored preference (`lib/i18n`), so flipping it here also changes the Fundamental modal
            opened from anywhere — which is the point: a language is a property of the reader, not
            of a screen. The cost is that it sits above pages that are NOT translated yet, where
            pressing it moves nothing; `i18n.ts` used to argue against exactly that and now records
            it as accepted. `managementCopy`'s `UNTRANSLATED_SURFACES` is the list of what still
            does not answer, so the gap is written down rather than discovered by pressing it. */}
        <div className="px-3 py-2 flex items-center justify-between gap-2">
          <span className="text-[11px] uppercase tracking-wider text-fg-subtle">{t.language}</span>
          <LangSwitch lang={lang} onChange={setLang} title={t.languageTitle} />
        </div>
        <button
          onClick={handleSignOut}
          className="w-full px-3 py-2.5 rounded-lg text-sm font-medium text-fg-subtle hover:text-fg-strong hover:bg-overlay/5 transition-colors text-left"
        >
          {t.signOut}
        </button>
        {!showDeleteConfirm ? (
          <button
            onClick={() => setShowDeleteConfirm(true)}
            className="w-full px-3 py-2.5 rounded-lg text-sm font-medium text-fg-subtle hover:text-neg-400 hover:bg-neg-500/10 transition-colors text-left"
          >
            {t.deleteAccount}
          </button>
        ) : (
          <div className="px-3 py-2 space-y-2">
            <p className="text-sm text-neg-400">{t.deleteSure}</p>
            <div className="flex gap-2">
              <button
                onClick={handleDeleteAccount}
                disabled={deleting}
                className="flex-1 px-2 py-1.5 rounded-lg text-sm font-medium bg-neg-600 hover:bg-neg-500 text-fg-strong transition-colors disabled:opacity-50"
              >
                {deleting ? t.deleting : t.deleteConfirm}
              </button>
              <button
                onClick={() => setShowDeleteConfirm(false)}
                className="flex-1 px-2 py-1.5 rounded-lg text-sm font-medium text-fg-muted hover:text-fg-strong hover:bg-overlay/5 transition-colors"
              >
                {t.cancel}
              </button>
            </div>
          </div>
        )}
      </div>
    </aside>
    </>
  );
}
