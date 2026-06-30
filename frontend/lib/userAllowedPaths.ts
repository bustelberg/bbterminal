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
export const USER_ALLOWED_PATHS: readonly string[] = ['/', '/earnings', '/schedule', '/forbidden'];

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
