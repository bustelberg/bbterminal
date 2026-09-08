// The ONLY routes a non-admin user (or an admin previewing via the "view as
// user" toggle) may open. SINGLE SOURCE OF TRUTH — shared by the route gate
// (`proxy.ts`), the home tiles (`app/page.tsx`), and the sidebar nav
// (`Sidebar.tsx`) so none of them can ever advertise a page the user can't
// actually navigate to. (They used to keep separate hand-maintained lists,
// which drifted: the home page advertised AIRS + Companies — both admin-only —
// while omitting /schedule, so a user clicking those tiles hit /forbidden.)
//
// `/` is the home page (any authenticated user); `/forbidden` renders the
// no-access page without a redirect loop.
//
// ⚠ THE PAGE, NOT ITS BUTTONS. `/management-dashboard` is readable by users; every mutation on it
// (Refresh, Delete, the Class/ISIN/Link overrides, the benchmark Fill) stays admin-only in BOTH
// places — the API gate refuses them, and the components hide the controls via `useIsAdmin` so a
// user is never shown a button that 403s.
//
// ⚠⚠ ADDING A PAGE HERE IS HALF THE JOB, AND SO IS REMOVING ONE. A page needs whatever it FETCHES
// allow-listed in `_auth_middleware.py`; put a page here without that and the user gets a screen of
// 403s, which reads as a broken app rather than as a permission. The reverse holds too — take a
// page away and the API paths it alone needed are still open, which is a permission nobody can see.
//
// `/research-dashboard` was the worked example of both halves (2026-09-07, removed on request).
// Its picker called `/api/asset-pipeline/search`, in a namespace otherwise admin-only — `/grid`
// alone is 27.56 MB of every ISIN, and `/ingest` and `/store` live there too — so that ONE path was
// allow-listed by EXACT pattern rather than by prefix. That entry went with the page.
//
// ⚠ `/earnings` WENT AT THE SAME TIME, AND `/api/earnings` DID NOT. The namespace stays in
// `_USER_READ_PREFIXES` because /management-dashboard is built on it: the Long Equity tab and the
// Fundamental modal read `fundamental-blend-metrics`, `universe-period-caps` and the eleven
// `*-inputs` endpoints. Closing the prefix to match the removed page would take that dashboard's
// charts down with it — the page and the API namespace are not the same permission.
//
// ⚠ AND SINCE 2026-08-06, ONE READ IS RESTRICTED TOO: expanding a row in the Overview table. The
// summary a user sees is the whole page for them; the book behind a row — positions and their EUR
// values, mutations, reconciliation — is admin's. Same two places (`PortfolioOverviewPanel`'s
// `expand` + `_ADMIN_ONLY_PATTERNS` in `_auth_middleware.py`), so the page staying in this list is
// not a statement that everything on it is readable.
// ⚠⚠ `/account` IS THE ONE ENTRY WITH NO SECOND HALF, AND THAT IS NOT AN OMISSION. Everything
// above says a page here also needs its API paths opened in `_auth_middleware.py` — but
// `/account/security` calls `supabase.auth.mfa.*`, which goes to `NEXT_PUBLIC_SUPABASE_URL`, not
// to our backend. It makes no `apiFetch` call at all, so there is nothing to allow-list and
// nothing that will 403. Stated here because the missing half otherwise reads as forgotten.
//
// ⚠ THE PREFIX, NOT THE LEAF. `/account` matches `/account/security` as a subroute, so a later
// `/account/profile` needs no edit here — and there is nothing under `/account` that could ever be
// admin-only: it is by definition the reader's own account.
export const USER_ALLOWED_PATHS: readonly string[] = [
  '/', '/schedule', '/management-dashboard', '/account', '/mfa', '/forbidden',
];

/**
 * True when a non-admin user may open `pathname`. Matches an entry exactly OR
 * as a subroute (prefix + '/'). '/' matches ONLY exactly — every path
 * startsWith('/'), so the subroute form would let everything through.
 */
export function isUserAllowedPath(pathname: string): boolean {
  return USER_ALLOWED_PATHS.some(
    (p) => pathname === p || (p !== '/' && pathname.startsWith(`${p}/`)),
  );
}
