import { createServerClient } from '@supabase/ssr'
import { cookies } from 'next/headers'
import { NextResponse, type NextRequest } from 'next/server'

export async function GET(request: NextRequest) {
  const { searchParams, origin } = new URL(request.url)
  const code = searchParams.get('code')
  const token_hash = searchParams.get('token_hash')
  const type = searchParams.get('type') as 'magiclink' | 'email' | null

  const cookieStore = await cookies()

  const supabase = createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        getAll() {
          return cookieStore.getAll()
        },
        setAll(cookiesToSet) {
          cookiesToSet.forEach(({ name, value, options }) =>
            cookieStore.set(name, value, options),
          )
        },
      },
    },
  )

  if (code) {
    await supabase.auth.exchangeCodeForSession(code)
  } else if (token_hash && type) {
    await supabase.auth.verifyOtp({ token_hash, type })
  }

  // `?next=...` lets callers (e.g. the impersonation flow) land on a
  // specific path after sign-in; default is /set-password for new-user
  // flows where we still want to force a permanent password.
  const next = searchParams.get('next')
  // Only allow same-origin paths (must start with `/` and not `//` to
  // prevent open-redirects).
  const safeNext = next && next.startsWith('/') && !next.startsWith('//') ? next : '/set-password'
  return NextResponse.redirect(`${origin}${safeNext}`)
}
