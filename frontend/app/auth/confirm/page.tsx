'use client'

import { Suspense, useState } from 'react'
import Link from 'next/link'
import { useRouter, useSearchParams } from 'next/navigation'
import { createBrowserClient } from '@supabase/ssr'
import { describeAuthError, hasAuthError } from '@/lib/authError'

/**
 * Where an email link lands — and it does NOT sign anyone in until a person clicks.
 *
 * ⚠⚠ THIS REPLACED A ROUTE HANDLER THAT VERIFIED ON GET, AND THAT IS THE WHOLE POINT. A sign-in
 * token is single-use, and *anything* that fetches the URL spends it. Corporate mail security
 * (Microsoft Defender Safe Links, Proofpoint, Mimecast, Gmail's image proxy) fetches every link in
 * every message before the recipient sees it — so by the time the human clicks, the token is gone
 * and they are told the link was already used. Measured in production 2026-08-19: exactly that.
 *
 * ⚠⚠ AND MOVING THE LINK OFF `{{ .ConfirmationURL }}` DOES NOT FIX IT. That change was worth
 * making — it removes the PKCE code verifier, which is what broke opening the mail on a different
 * device from the one that requested it — but `/auth/v1/verify` and our own `/auth/confirm` are
 * both plain GETs, so a scanner consumes the token either way. It moved WHO spends the token, not
 * WHETHER a scanner can. The fix has to be that a GET spends nothing.
 *
 * So: the page reads the token out of the URL and does nothing with it. `verifyOtp` runs on a
 * BUTTON PRESS. A scanner issues the GET, renders no JS, presses nothing, and the token is still
 * there when the person arrives. It also fixes the quieter version of the same bug — a back button
 * or a refresh re-running the verification and reporting a link that worked as already used.
 *
 * ⚠ `detectSessionInUrl: false` IS LOAD-BEARING, NOT TIDINESS. The default browser client
 * processes `?code=` automatically on page load, which would consume the token before render and
 * hand the whole protection back. This page needs its own client for that one option.
 */

/** The `type` values Supabase can send on an email OTP. ⚠ NOT narrowed to two, as the route once
 *  was: `invite`, `recovery` and `signup` all arrive here. */
const OTP_TYPES = ['magiclink', 'email', 'signup', 'invite', 'recovery', 'email_change'] as const
type OtpType = (typeof OTP_TYPES)[number]

const isOtpType = (v: string | null): v is OtpType =>
  Boolean(v) && (OTP_TYPES as readonly string[]).includes(v as string)

/** ⚠ `useSearchParams` forces a Suspense boundary, or the whole route opts out of static rendering. */
export default function ConfirmPage() {
  return (
    <Suspense fallback={null}>
      <Confirm />
    </Suspense>
  )
}

function Confirm() {
  const router = useRouter()
  const params = useSearchParams()

  // ⚠ ITS OWN CLIENT, ONCE. See the header: the shared `createClient()` auto-detects `?code=` in
  // the URL, which would verify on load and defeat the button.
  const [supabase] = useState(() => createBrowserClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    { auth: { detectSessionInUrl: false } },
  ))

  const code = params.get('code')
  const tokenHash = params.get('token_hash')
  const type = params.get('type')

  /**
   * The reason this link cannot work, decided at RENDER — no effect, no setState-in-effect.
   *
   * ⚠ THE `error*` PARAMS ARE CHECKED FIRST. A token Supabase has already rejected comes back with
   * `error=…` and NO `code` and NO `token_hash`, so anything that only looks for those two sees an
   * empty query and concludes there is nothing to do.
   */
  const upfront = hasAuthError(params)
    ? describeAuthError({
      error: params.get('error'),
      errorCode: params.get('error_code'),
      errorDescription: params.get('error_description'),
    })
    : (!code && !tokenHash)
      // ⚠ AN EMPTY QUERY IS A FAILURE, NOT A NO-OP. It is also what an implicit-flow link produces:
      // the tokens arrive in the URL *fragment*, and this page is not built to read one.
      ? 'That link did not carry a sign-in token. Request a new one from the login page.'
      : (tokenHash && !isOtpType(type))
        ? 'That link is missing what kind of confirmation it is. Request a new one from the login page.'
        : null

  const [failed, setFailed] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const problem = upfront ?? failed

  async function confirm() {
    setBusy(true)
    setFailed(null)
    try {
      const { error } = tokenHash && isOtpType(type)
        // ⚠ `token_hash` FIRST. `?code=` is PKCE — only the browser that REQUESTED the link holds
        // the verifier, so a phone opening a laptop's email cannot complete it. `verifyOtp` carries
        // no such state.
        ? await supabase.auth.verifyOtp({ token_hash: tokenHash, type })
        : await supabase.auth.exchangeCodeForSession(code!)
      if (error) {
        console.warn('[auth/confirm] verification failed:', error)
        setFailed(describeAuthError({ message: error.message }))
        setBusy(false)
        return
      }
      // ⚠ PROVE IT RATHER THAN ASSUME IT. A verify can report success while the session fails to
      // reach the cookie jar, which lands someone on the password form with nothing behind it —
      // the exact state this whole flow exists to prevent.
      const { data: { user } } = await supabase.auth.getUser()
      if (!user) {
        setFailed('Signing in from that link did not stick. Request a new one from the login page.')
        setBusy(false)
        return
      }
    } catch (e) {
      console.warn('[auth/confirm] verification threw:', e)
      setFailed(describeAuthError({ message: e instanceof Error ? e.message : String(e) }))
      setBusy(false)
      return
    }
    // `?next=` lets a caller land somewhere specific; default is /set-password for new accounts.
    // ⚠ Same-origin paths only (leading `/`, not `//`) — otherwise this is an open redirect.
    const next = params.get('next')
    const safeNext = next && next.startsWith('/') && !next.startsWith('//') ? next : '/set-password'
    // `replace`, so Back does not return to a link that has now genuinely been used.
    router.replace(safeNext)
  }

  return (
    <div className="min-h-screen bg-scrim flex items-center justify-center p-6">
      <div className="w-full max-w-sm border border-neutral-800 rounded p-8 space-y-4">
        <h1 className="font-mono text-base font-bold text-fg-strong">BBTerminal</h1>

        {problem ? (
          <>
            <p className="font-mono text-xs text-neg-400 leading-relaxed">{problem}</p>
            <Link href="/login"
              className="block text-center bg-neutral-700 hover:bg-neutral-600 text-fg-strong font-mono text-sm rounded px-4 py-2 transition-colors">
              Request a new link
            </Link>
          </>
        ) : (
          <>
            <p className="font-mono text-xs text-fg-subtle leading-relaxed">
              Press the button to confirm your email address and continue.
            </p>
            <button type="button" onClick={confirm} disabled={busy}
              className="w-full bg-neutral-700 hover:bg-neutral-600 disabled:opacity-50 text-fg-strong font-mono text-sm rounded px-4 py-2 transition-colors">
              {busy ? 'Confirming…' : 'Confirm my email'}
            </button>
            {/* ⚠ SAID OUT LOUD. Without a sentence here the extra click reads as a pointless step,
                and the first thing anyone does with a pointless step is try to remove it. */}
            <p className="font-mono text-[10px] text-fg-faint leading-relaxed">
              This step is here because mail scanners open links automatically. Nothing is used up
              until you press the button.
            </p>
          </>
        )}
      </div>
    </div>
  )
}
