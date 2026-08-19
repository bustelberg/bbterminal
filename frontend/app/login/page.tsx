'use client'

import { Suspense, useState } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { createClient } from '../../lib/supabase/client'
import { describeSendError } from '../../lib/authError'

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
        setInfo('Check your email for a confirmation link — including the spam folder. '
          + 'You will be asked to press a button to confirm, then choose a password.')
      }
    } else {
      const { error } = await supabase.auth.signInWithPassword({ email, password })
      if (error) {
        setError(error.message)
      } else {
        router.push('/')
        router.refresh()
      }
    }

    setLoading(false)
  }

  return (
    <div className="min-h-screen bg-page flex items-center justify-center">
      <div className="w-full max-w-sm bg-card border border-neutral-800/40 rounded-xl p-8">
        <h1 className="text-lg font-semibold text-fg-strong mb-1">BBTerminal</h1>
        <p className="text-sm text-fg-subtle mb-6">
          {mode === 'signin' ? 'Sign in to your account' : 'Request access'}
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-xs font-medium text-fg-muted mb-1.5">Email</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
              autoComplete="email"
              className="w-full bg-page border border-neutral-700 rounded-lg px-3 py-2.5 text-sm text-fg-strong placeholder-fg-faint focus:outline-none focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 transition-colors"
              placeholder="you@bustelberg.nl"
            />
          </div>

          {mode === 'signin' && (
            <div>
              <label className="block text-xs font-medium text-fg-muted mb-1.5">Password</label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                autoComplete="current-password"
                className="w-full bg-page border border-neutral-700 rounded-lg px-3 py-2.5 text-sm text-fg-strong placeholder-fg-faint focus:outline-none focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 transition-colors"
                placeholder="••••••••"
              />
            </div>
          )}

          {error && <p className="text-xs text-neg-400">{error}</p>}
          {info  && <p className="text-xs text-pos-400">{info}</p>}

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-accent-600 hover:bg-accent-500 disabled:opacity-50 text-fg-strong text-sm font-medium rounded-lg px-4 py-2.5 transition-colors"
          >
            {loading
              ? 'Please wait...'
              : mode === 'signin'
              ? 'Sign in'
              : 'Send confirmation link'}
          </button>
        </form>

        <p className="mt-5 text-xs text-fg-faint text-center">
          {mode === 'signin' ? 'New user?' : 'Already have an account?'}{' '}
          <button
            onClick={() => {
              setMode(mode === 'signin' ? 'signup' : 'signin')
              setError(null)
              setInfo(null)
            }}
            className="text-accent-400 hover:text-accent-300 transition-colors"
          >
            {mode === 'signin' ? 'Request access' : 'Sign in'}
          </button>
        </p>
      </div>
    </div>
  )
}
