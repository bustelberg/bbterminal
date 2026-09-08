import { createServerClient } from '@supabase/ssr'
import { NextResponse, type NextRequest } from 'next/server'
import { isUserAllowedPath } from '@/lib/userAllowedPaths'
import {
  ENROL_PATH, MFA_PATH, mfaEnforced, requiresEnrolment, requiresMfa, type AalLevel,
} from '@/lib/mfaGate'

// Paths that are accessible to anyone — including not-yet-logged-in users
// (auth flow) and the home page (which any authenticated user can see).
const PUBLIC_PATH_PREFIXES: readonly string[] = ['/login', '/set-password', '/auth/']

function isPublicPath(pathname: string): boolean {
  return PUBLIC_PATH_PREFIXES.some((p) => pathname === p || pathname.startsWith(p))
}

// ⚠ THERE IS NO AUTH BYPASS, AND THERE MUST NOT BE ONE AGAIN. `E2E_BYPASS_AUTH` used to
// short-circuit this function so Playwright could reach any route without a login. The e2e suite
// is gone (unit tests only — see CLAUDE.md), and with it the only reason this file ever held an
// env-var-controlled way to switch authentication off. Anything that needs to test around auth
// should test a pure function, not disable the gate.
export async function proxy(request: NextRequest) {
  let supabaseResponse = NextResponse.next({ request })

  const supabase = createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        getAll() {
          return request.cookies.getAll()
        },
        setAll(cookiesToSet) {
          cookiesToSet.forEach(({ name, value }) =>
            request.cookies.set(name, value),
          )
          supabaseResponse = NextResponse.next({ request })
          cookiesToSet.forEach(({ name, value, options }) =>
            supabaseResponse.cookies.set(name, value, options),
          )
        },
      },
    },
  )

  // Refresh session — must call getUser() not getSession()
  const { data: { user } } = await supabase.auth.getUser()

  const { pathname } = request.nextUrl
  const publicPath = isPublicPath(pathname)

  if (!user && !publicPath) {
    const url = request.nextUrl.clone()
    url.pathname = '/login'
    return NextResponse.redirect(url)
  }

  if (user && pathname === '/login') {
    const url = request.nextUrl.clone()
    url.pathname = '/'
    return NextResponse.redirect(url)
  }

  /**
   * ⚠⚠ THE SECOND-FACTOR GATE, AND IT RUNS BEFORE THE ROLE GATE ON PURPOSE. A session that has
   * not proved its factor should be sent to the challenge, not told it lacks permission — the
   * other order answers "you may not see this" to somebody who may, and hides the one action that
   * would let them through.
   *
   * ⚠ `getAuthenticatorAssuranceLevel()` DECODES THE SESSION LOCALLY — no round trip. It reads the
   * `aal` claim out of the JWT already in the cookies, which `getUser()` above has just validated
   * against the auth server, so this adds nothing to the latency of every request.
   *
   * ⚠⚠ TWO-FACTOR IS MANDATORY, NOT OFFERED (2026-09-08, on request; it was opt-in for one
   * afternoon). Two rules, in this order — no authenticator at all sends you to SET ONE UP, and
   * an unproved one sends you to the CHALLENGE. Getting them the other way round strands a new
   * account at a code box with nothing that can produce a code.
   */
  // ⚠ `mfaEnforced()` IS A COMPILE-TIME CONSTANT IN PRODUCTION — see its docstring. This whole
  // block is dev-only escapable; on Vercel no environment variable can reach it.
  if (user && mfaEnforced()) {
    const { data: aal } = await supabase.auth.mfa.getAuthenticatorAssuranceLevel()
    const levels = {
      currentLevel: aal?.currentLevel as AalLevel,
      nextLevel: aal?.nextLevel as AalLevel,
      pathname,
      isPublic: publicPath,
    }

    /**
     * ⚠⚠ ENROLMENT IS CHECKED FIRST, AND THE ORDER IS THE WHOLE DIFFERENCE BETWEEN "REQUIRED" AND
     * "AVAILABLE". Somebody with no authenticator cannot satisfy a challenge, so sending them to
     * `/mfa` would be a dead end; they need the SET-UP page. Two-factor is mandatory here — a new
     * account is stopped at enrolment before it can reach anything else.
     *
     * ⚠ NO `?next=`. Enrolling is not a detour on the way somewhere — it is a thing to finish, and
     * a redirect firing the moment the factor verifies would snatch the page away mid-sentence,
     * before the reader has seen that it worked or read the recovery warning.
     */
    if (requiresEnrolment(levels)) {
      const url = request.nextUrl.clone()
      url.pathname = ENROL_PATH
      url.search = ''
      return NextResponse.redirect(url)
    }

    if (requiresMfa(levels)) {
      const url = request.nextUrl.clone()
      url.pathname = MFA_PATH
      // Carry where they were going, so proving the factor resumes the journey instead of
      // dumping everyone on the home page.
      url.search = `?next=${encodeURIComponent(pathname + request.nextUrl.search)}`
      return NextResponse.redirect(url)
    }
  }

  if (user) {
    // Role gate. The role lives in `auth.users.raw_app_meta_data.role`
    // (set by the 20260502000000_admin_role.sql migration for the admin
    // email; everyone else is implicit 'user'). Admins can opt into
    // user-view via the `view_as=user` cookie set by the sidebar toggle —
    // useful for verifying what regular users actually see.
    const appMeta = (user.app_metadata ?? {}) as { role?: string }
    const realRole = appMeta.role === 'admin' ? 'admin' : 'user'
    const viewAs = request.cookies.get('view_as')?.value
    const effectiveRole = realRole === 'admin' && viewAs === 'user' ? 'user' : realRole

    if (effectiveRole !== 'admin' && !isUserAllowedPath(pathname) && !publicPath) {
      // Regular user (or admin in view-as mode) hit an admin-only path —
      // route them to /forbidden so the URL stays explicit about what
      // happened (instead of silently bouncing to '/'). Pass the original
      // path as a search param so the forbidden page can name it.
      const url = request.nextUrl.clone()
      url.pathname = '/forbidden'
      url.search = `?from=${encodeURIComponent(pathname)}`
      return NextResponse.redirect(url)
    }
  }

  return supabaseResponse
}

export const config = {
  matcher: [
    '/((?!_next/static|_next/image|favicon\\.ico|.*\\.(?:svg|png|jpg|jpeg|gif|webp)$).*)',
  ],
}
