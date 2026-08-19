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
// ⚠ `/research-dashboard` NEEDED ONE LINE IN THE API GATE TOO, and the page being here is not what
// makes it work. Its picker calls `/api/asset-pipeline/search`, which sits in a namespace that is
// otherwise admin-only — `/grid` alone is 27.56 MB of every ISIN, and `/ingest` and `/store` live
// there as well. So the search path is allow-listed by EXACT pattern in `_auth_middleware.py`, not
// by prefix. Adding a page to this list without checking what it FETCHES gives a user a page of
// 403s, which reads as a broken app rather than as a permission.
//
// ⚠ AND SINCE 2026-08-06, ONE READ IS RESTRICTED TOO: expanding a row in the Overview table. The
// summary a user sees is the whole page for them; the book behind a row — positions and their EUR
// values, mutations, reconciliation — is admin's. Same two places (`PortfolioOverviewPanel`'s
// `expand` + `_ADMIN_ONLY_PATTERNS` in `_auth_middleware.py`), so the page staying in this list is
// not a statement that everything on it is readable.
export const USER_ALLOWED_PATHS: readonly string[] = [
  '/', '/earnings', '/schedule', '/management-dashboard', '/research-dashboard', '/forbidden',
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
