import { createServerClient } from '@supabase/ssr'
import { cookies } from 'next/headers'
import { NextResponse, type NextRequest } from 'next/server'
import { describeAuthError, hasAuthError } from '@/lib/authError'

/**
 * Where an email link lands: turn it into a session, or say why it could not be.
 *
 * ⚠⚠ EVERY FAILURE USED TO END AT `/set-password` ANYWAY. This route awaited
 * `exchangeCodeForSession` / `verifyOtp` and DISCARDED the result, then redirected to the password
 * form whatever had happened. So an expired link, a link already opened by a mail scanner, and a
 * link opened in a different browser all produced the same thing: a working-looking password form,
 * and — only after the person had chosen a password and pressed Save — `updateUser` failing with
 * "Auth session missing". That string names the symptom (no session) and nothing about the cause,
 * at the one screen where the cause could not possibly be fixed.
 *
 * Now: a failure never reaches `/set-password`. It goes back to `/login` carrying a sentence that
 * says what to do (`describeAuthError`), and the full detail goes to the server log.
 *
 * ⚠ `token_hash` IS TRIED BEFORE `code`, AND THAT ORDER IS THE FIX FOR THE CROSS-DEVICE CASE.
 * `?code=` is PKCE: the browser that ASKED for the link stored a code verifier in a cookie, and
 * only that browser can complete the exchange. Request the link on a laptop, open the mail on a
 * phone, and it cannot work — which is invisible in development, where there is only ever one
 * browser. `token_hash` + `verifyOtp` carries no such state and works from anywhere.
 *
 * ⚠⚠ FOR THAT PATH TO BE TAKEN, THE EMAIL TEMPLATE HAS TO SEND IT. Supabase's DEFAULT template is
 * `{{ .ConfirmationURL }}`, which points at `/auth/v1/verify` — that URL consumes the one-time
 * token when it is fetched, which is why a mail scanner opening it first kills the link before the
 * human clicks. Point the template at this route instead:
 *
 *     {{ .SiteURL }}/auth/confirm?token_hash={{ .TokenHash }}&type={{ .Email_Action_Type }}
 *
 * Both paths are kept because the template is project configuration, not code: this route must
 * keep working on a project where it has not been changed yet.
 */

/** The `type` values Supabase can send on an email OTP. ⚠ NOT narrowed to two, as it used to be:
 *  `invite`, `recovery` and `signup` all arrive here, and the old cast said they could not. */
const OTP_TYPES = ['magiclink', 'email', 'signup', 'invite', 'recovery', 'email_change'] as const
type OtpType = (typeof OTP_TYPES)[number]

const isOtpType = (v: string | null): v is OtpType =>
  Boolean(v) && (OTP_TYPES as readonly string[]).includes(v as string)

export async function GET(request: NextRequest) {
  const { searchParams, origin } = new URL(request.url)

  /** Back to the login page, with one short line the person can act on. */
  const fail = (why: string, detail: unknown) => {
    // ⚠ THE DIAGNOSTIC GOES HERE, NOT ON SCREEN. Same rule as everywhere else in this app: the
    // console/log gets everything, the UI gets a sentence. Without this line a production failure
    // leaves no trace at all — which is how the swallowed version survived for as long as it did.
    console.warn('[auth/confirm] failed:', detail, Object.fromEntries(searchParams))
    const url = new URL('/login', origin)
    url.searchParams.set('error', why)
    return NextResponse.redirect(url)
  }

  // ⚠ FIRST, BEFORE ANYTHING ELSE. A rejected token comes back as `?error=...` with no `code` and
  // no `token_hash`, so a route that only looks for those two sees an empty query and sails on.
  if (hasAuthError(searchParams)) {
    return fail(
      describeAuthError({
        error: searchParams.get('error'),
        errorCode: searchParams.get('error_code'),
        errorDescription: searchParams.get('error_description'),
      }),
      'supabase returned an error on the redirect',
    )
  }

  const code = searchParams.get('code')
  const tokenHash = searchParams.get('token_hash')
  const type = searchParams.get('type')

  if (!code && !tokenHash) {
    // ⚠ AN EMPTY QUERY IS A FAILURE, NOT A NO-OP. It is also what an implicit-flow link produces:
    // the tokens arrive in the URL *fragment*, which browsers never send to a server, so this
    // route is structurally unable to see them. Carrying on to `/set-password` would be the old
    // bug in its purest form — no session, no error, no clue.
    return fail(
      'That link did not carry a sign-in token. Request a new one below.',
      'no code and no token_hash in the callback URL',
    )
  }

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

  // ⚠ `token_hash` FIRST — see the header. It is the path that works from any device.
  if (tokenHash) {
    if (!isOtpType(type)) {
      return fail(
        'That link is missing what kind of confirmation it is. Request a new one below.',
        `unknown otp type ${JSON.stringify(type)}`,
      )
    }
    const { error } = await supabase.auth.verifyOtp({ token_hash: tokenHash, type })
    if (error) return fail(describeAuthError({ message: error.message }), error)
  } else if (code) {
    const { error } = await supabase.auth.exchangeCodeForSession(code)
    if (error) return fail(describeAuthError({ message: error.message }), error)
  }

  // ⚠ PROVE IT RATHER THAN ASSUME IT. The calls above can report success while the session still
  // fails to reach the browser — a cookie that did not make it onto this redirect leaves exactly
  // the state this route exists to prevent. One read costs a round trip and turns a silent
  // "Auth session missing" two screens later into an answer here.
  const { data: { user }, error: userErr } = await supabase.auth.getUser()
  if (userErr || !user) {
    return fail(
      'Signing in from that link did not stick. Try again below.',
      userErr ?? 'no user after a successful verify',
    )
  }

  // `?next=...` lets callers land on a specific path after sign-in; default is /set-password for
  // new-user flows where we still want to force a permanent password.
  const next = searchParams.get('next')
  // Only allow same-origin paths (must start with `/` and not `//` to prevent open-redirects).
  const safeNext = next && next.startsWith('/') && !next.startsWith('//') ? next : '/set-password'
  return NextResponse.redirect(`${origin}${safeNext}`)
}
