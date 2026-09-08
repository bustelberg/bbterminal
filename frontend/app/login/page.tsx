'use client'

import { Suspense, useState } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { createClient } from '../../lib/supabase/client'
import { describeSendError } from '../../lib/authError'
import AuthShell, {
  AuthNotice,
  authButtonClass,
  authFieldClass,
  authLabelClass,
} from '../components/auth/AuthShell'

const ALLOWED_DOMAIN = 'bustelberg.nl'
const ALLOWED_EMAILS = (process.env.NEXT_PUBLIC_ALLOWED_EMAILS ?? '')
  .split(',')
  .map((e) => e.trim().toLowerCase())
  .filter(Boolean)

function isAllowed(email: string): boolean {
  const lower = email.toLowerCase()
  return lower.endsWith(`@${ALLOWED_DOMAIN}`) || ALLOWED_EMAILS.includes(lower)
}

/**
 * ⚠ `useSearchParams` FORCES A SUSPENSE BOUNDARY, and without one the whole route opts out of
 * static rendering. One wrapper here is cheaper than a build-time error nobody expects on a page
 * this simple.
 */
export default function LoginPage() {
  return (
    <Suspense fallback={null}>
      <LoginForm />
    </Suspense>
  )
}

function LoginForm() {
  const router = useRouter()
  const params = useSearchParams()
  const [supabase] = useState(() => createClient())

  /**
   * ⚠⚠ THE SENTENCE `/auth/confirm` SENDS WHEN A LINK FAILS. Before this it sent nothing and the
   * failure was invisible: the route redirected to `/set-password` whatever had happened, and the
   * person met "Auth session missing" after choosing a password. The message is composed there
   * (`describeAuthError`), so this page only has to show it — and it opens in signup mode, because
   * every one of those failures is somebody trying to get IN, and what they need is another link.
   *
   * ⚠ SEEDED INTO `useState`, NOT SET FROM AN EFFECT. It is derived from the URL, which is known at
   * first render; assigning it in an effect renders once with no message and again with it, and is
   * what `react-hooks/set-state-in-effect` is pointing at. The user can still dismiss it by
   * submitting — that is why it is state at all and not a plain constant.
   */
  const linkError = params.get('error')

  const [mode, setMode] = useState<'signin' | 'signup'>(linkError ? 'signup' : 'signin')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(linkError)
  const [info, setInfo] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError(null)
    setInfo(null)

    // Only restrict the *signup* path — admins can invite arbitrary users via
    // /users, and once they're in the DB they should be able to sign in
    // regardless of email domain. Sign-in failures for unknown accounts are
    // still rejected by Supabase Auth itself.
    if (mode === 'signup' && !isAllowed(email)) {
      setError(`Self-signup is restricted to @${ALLOWED_DOMAIN} accounts. Ask the admin to invite you instead.`)
      return
    }

    setLoading(true)

    if (mode === 'signup') {
      // Send magic link — user clicks it, confirms ownership, then sets a password
      const { error } = await supabase.auth.signInWithOtp({
        email,
        options: {
          emailRedirectTo: `${window.location.origin}/auth/confirm`,
          shouldCreateUser: true,
        },
      })
      if (error) {
        // ⚠ THE RAW MESSAGE IS "email rate limit exceeded", WHICH READS AS A BUG. The built-in mail
        // service is capped at 2/hour PROJECT-WIDE and cannot be raised without custom SMTP — and
        // every failure path in this flow ends by telling someone to request another link, so the
        // third one silently cannot be sent. See `describeSendError`.
        console.warn('[login] send failed:', error)
        setError(describeSendError(error.message))
      } else {
        setInfo('Check your email for a confirmation link, including the spam folder. '
          + 'You will be asked to press a button to confirm, then choose a password.')
      }
    } else {
      const { error } = await supabase.auth.signInWithPassword({ email, password })
      if (error) {
        // ⚠ FULL DETAIL TO THE CONSOLE, ONE SENTENCE TO THE SCREEN — the house rule, and this was
        // the last place in the flow still printing a library string. "Invalid login credentials"
        // is Supabase deliberately refusing to say WHICH half was wrong (saying so would enumerate
        // accounts), so the screen has to turn that into something a person can act on.
        console.warn('[login] sign-in failed:', error)
        setError(/invalid login credentials/i.test(error.message)
          ? 'That email and password did not match. Check them, or email yourself a sign-in link '
            + 'below if you have not chosen a password yet.'
          : error.message)
      } else {
        router.push('/')
        router.refresh()
      }
    }

    setLoading(false)
  }

  const signin = mode === 'signin'

  function switchMode() {
    setMode(signin ? 'signup' : 'signin')
    setError(null)
    setInfo(null)
  }

  return (
    <AuthShell
      title={signin ? 'Welcome back' : 'Request access'}
      subtitle={signin
        ? 'Sign in to continue'
        : 'We will email you a link to confirm your address. No password needed yet.'}
      footer={
        <>
          {signin ? 'First time here?' : 'Already have an account?'}{' '}
          <button
            type="button"
            onClick={switchMode}
            className="font-medium text-accent-400 hover:text-accent-500 underline underline-offset-2 transition-colors"
          >
            {signin ? 'Request access' : 'Sign in instead'}
          </button>
        </>
      }
    >
      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label htmlFor="email" className={authLabelClass}>Email</label>
          <input
            id="email"
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
            autoComplete="email"
            autoFocus
            className={authFieldClass}
            placeholder="you@bustelberg.nl"
          />
        </div>

        {signin && (
          <div>
            <label htmlFor="password" className={authLabelClass}>Password</label>
            <input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              autoComplete="current-password"
              className={authFieldClass}
              placeholder="••••••••"
            />
          </div>
        )}

        {error && <AuthNotice kind="error">{error}</AuthNotice>}
        {info && <AuthNotice kind="info">{info}</AuthNotice>}

        <button type="submit" disabled={loading} className={authButtonClass}>
          {loading
            ? (signin ? 'Signing in…' : 'Sending…')
            : signin
              ? 'Sign in'
              : 'Email me a link'}
        </button>
      </form>

      {/* ⚠ SIGN-IN MODE ONLY. In signup mode the button above already sends a link, so offering a
          second way to ask for one beside it is two controls doing one thing. */}
      {signin && (
        <p className="mt-4 text-xs text-fg-faint text-center leading-relaxed">
          Forgotten your password?{' '}
          <button
            type="button"
            onClick={switchMode}
            className="text-accent-400 hover:text-accent-500 underline underline-offset-2 transition-colors"
          >
            Email yourself a sign-in link
          </button>{' '}
          and choose a new one.
        </p>
      )}
    </AuthShell>
  )
}
