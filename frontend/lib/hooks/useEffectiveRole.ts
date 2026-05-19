'use client';

import { useEffect, useState } from 'react';
import { createClient } from '../supabase/client';

/**
 * Returns the effective role for the currently signed-in user, honoring
 * the admin "view as regular user" toggle (the `view_as=user` cookie set
 * by the Sidebar). Use this anywhere a component needs to render
 * differently for admins vs regular users — most commonly to hide
 * mutation controls.
 *
 *   - `null`    — still loading the user (component should render a
 *                 safe default; usually treat as non-admin).
 *   - `'admin'` — real admin AND not impersonating a user view.
 *   - `'user'`  — regular user, OR admin currently in view-as-user mode.
 *
 * The hook is intentionally one-shot (no subscription to auth-state
 * changes) — pages typically re-mount on navigation anyway, and the
 * Sidebar's view-as toggle does a hard reload on flip so the role
 * picks up the change naturally.
 */
export function useEffectiveRole(): 'admin' | 'user' | null {
  const [role, setRole] = useState<'admin' | 'user' | null>(null);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const supabase = createClient();
        const { data: { user } } = await supabase.auth.getUser();
        if (cancelled) return;
        if (!user) {
          setRole(null);
          return;
        }
        const meta = (user.app_metadata ?? {}) as { role?: string };
        const realRole: 'admin' | 'user' = meta.role === 'admin' ? 'admin' : 'user';
        // `view_as=user` cookie lets admins preview the user-only UI.
        const viewAsUser = typeof document !== 'undefined'
          && document.cookie.split('; ').some((c) => c.startsWith('view_as=user'));
        setRole(realRole === 'admin' && !viewAsUser ? 'admin' : 'user');
      } catch {
        if (!cancelled) setRole(null);
      }
    })();
    return () => { cancelled = true; };
  }, []);

  return role;
}

/**
 * Convenience boolean for the common case "should this admin-only
 * control render?". Returns `false` while the role is still loading
 * (safe-default: assume non-admin).
 */
export function useIsAdmin(): boolean {
  return useEffectiveRole() === 'admin';
}
