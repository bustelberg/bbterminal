import { createServerClient } from '@supabase/ssr'
import { NextResponse, type NextRequest } from 'next/server'
import { isAuthBypassEnabled } from '@/lib/authBypass'

// Paths a regular (non-admin) user is allowed to view. Everything else
// requires admin. Path matches against either an exact equality OR a
// prefix-with-trailing-slash so subroutes are also allowed when needed.
// `/forbidden` is here so blocked attempts can render the "no access"
// page without bouncing into another redirect loop.
const USER_ALLOWED_PATHS: readonly string[] = ['/', '/earnings', '/companies', '/airs-portfolio', '/forbidden']
// Paths that are accessible to anyone — including not-yet-logged-in users
// (auth flow) and the home page (which any authenticated user can see).
const PUBLIC_PATH_PREFIXES: readonly string[] = ['/login', '/set-password', '/auth/']

function pathAllowedFor(pathname: string, allowed: readonly string[]): boolean {
  for (const p of allowed) {
    if (pathname === p) return true
    // '/' must only match exactly — every path startsWith('/'), so the
    // subroute form would let any pathname through and defeat the gate.
    if (p !== '/' && pathname.startsWith(`${p}/`)) return true
  }
  return false
}

function isPublicPath(pathname: string): boolean {
  return PUBLIC_PATH_PREFIXES.some((p) => pathname === p || pathname.startsWith(p))
}

export async function proxy(request: NextRequest) {
  // Playwright e2e short-circuit. Fires only with `E2E_BYPASS_AUTH=1`
  // (set by `playwright.config.ts` / CI, never in dev or on Vercel) AND
  // when NOT running on Vercel (the `VERCEL` env var is the kill-switch),
  // so a stray prod env var can never disable auth on a real deployment.
  // Skips the Supabase server-side session call so tests can hit any
  // route without a real login; tests mock /api/* via `page.route()`.
  if (isAuthBypassEnabled(process.env.E2E_BYPASS_AUTH, process.env.VERCEL)) {
    return NextResponse.next({ request })
  }

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

    if (effectiveRole !== 'admin' && !pathAllowedFor(pathname, USER_ALLOWED_PATHS) && !publicPath) {
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
