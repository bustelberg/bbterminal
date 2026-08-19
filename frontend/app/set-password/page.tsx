'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { useRouter } from 'next/navigation'
import { createClient } from '../../lib/supabase/client'

/**
 * Choose a permanent password — reachable only with a live session from `/auth/confirm`.
 *
 * ⚠⚠ IT USED TO ASSUME THE SESSION AND FIND OUT AT SUBMIT TIME. The page rendered its form
 * unconditionally and called `updateUser({ password })`; with no session that fails with
 * "Auth session missing" — a library string naming the symptom, shown only AFTER someone had
 * chosen a password, typed it twice and pressed Save, on the one screen where nothing they could
 * do would fix it. `/auth/confirm` no longer sends failures here at all, but this page must not
 * depend on that: a bookmarked URL, a back button or a session that expired between the two lands
 * here too, and each of those deserves the same answer up front.
 */
export default function SetPasswordPage() {
  const router = useRouter()
  const [supabase] = useState(() => createClient())

  const [password, setPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  // `null` = still checking. ⚠ THREE STATES, NOT TWO: rendering the form while the answer is
  // unknown is how someone starts typing into a form that is about to be replaced.
  const [signedIn, setSignedIn] = useState<boolean | null>(null)

  useEffect(() => {
    let alive = true
    void (async () => {
      // ⚠ `getUser`, NOT `getSession` — the same rule `proxy.ts` states. `getSession` returns
      // whatever is in storage without asking whether it is still valid, so an expired session
      // would render the form and fail at submit exactly as before.
      const { data: { user } } = await supabase.auth.getUser()
      if (alive) setSignedIn(Boolean(user))
    })()
    return () => { alive = false }
  }, [supabase])

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError(null)

    if (password !== confirm) {
      setError('Passwords do not match.')
      return
    }
    if (password.length < 8) {
      setError('Password must be at least 8 characters.')
      return
    }

    setLoading(true)
    const { error } = await supabase.auth.updateUser({ password })
    if (error) {
      // ⚠ THE RAW MESSAGE GOES TO THE CONSOLE, A SENTENCE TO THE SCREEN. If the session went away
      // between the check on mount and this submit, "Auth session missing" is still the string
      // Supabase returns — and it is still not something a person can act on.
      console.warn('[set-password] updateUser failed:', error)
      setError(/session|jwt/i.test(error.message)
        ? 'Your sign-in link expired while this page was open. Request a new one from the login page.'
        : error.message)
      setLoading(false)
    } else {
      router.push('/')
      router.refresh()
    }
  }

  if (signedIn === null) {
    return (
      <div className="min-h-screen bg-scrim flex items-center justify-center">
        <p className="font-mono text-xs text-fg-subtle">Checking your sign-in link…</p>
      </div>
    )
  }

  // ⚠ NO PASSWORD FORM WITHOUT A SESSION. Offering one would be offering an action that cannot
  // succeed — and the failure would arrive after the work, phrased as a fault in the password.
  if (!signedIn) {
    return (
      <div className="min-h-screen bg-scrim flex items-center justify-center">
        <div className="w-full max-w-sm border border-neutral-800 rounded p-8 space-y-4">
          <h1 className="font-mono text-base font-bold text-fg-strong">BBTerminal</h1>
          <p className="font-mono text-xs text-fg-subtle leading-relaxed">
            You are not signed in, so there is no account to set a password on yet. Sign-in links
            work once, expire after an hour, and must be opened in the same browser you requested
            them from.
          </p>
          <Link href="/login"
            className="block text-center bg-neutral-700 hover:bg-neutral-600 text-fg-strong font-mono text-sm rounded px-4 py-2 transition-colors">
            Request a new link
          </Link>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-scrim flex items-center justify-center">
      <div className="w-full max-w-sm border border-neutral-800 rounded p-8">
        <h1 className="font-mono text-base font-bold text-fg-strong mb-1">BBTerminal</h1>
        <p className="font-mono text-xs text-fg-subtle mb-6">
          Choose a password for your account
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block font-mono text-xs text-fg-muted mb-1">Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              autoComplete="new-password"
              className="w-full bg-neutral-900 border border-neutral-700 rounded px-3 py-2 text-sm font-mono text-fg-strong placeholder-fg-faint focus:outline-none focus:border-neutral-500"
              placeholder="Min. 8 characters"
            />
          </div>
          <div>
            <label className="block font-mono text-xs text-fg-muted mb-1">
              Confirm password
            </label>
            <input
              type="password"
              value={confirm}
              onChange={(e) => setConfirm(e.target.value)}
              required
              autoComplete="new-password"
              className="w-full bg-neutral-900 border border-neutral-700 rounded px-3 py-2 text-sm font-mono text-fg-strong placeholder-fg-faint focus:outline-none focus:border-neutral-500"
              placeholder="••••••••"
            />
          </div>

          {error && <p className="font-mono text-xs text-red-400">{error}</p>}

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-neutral-700 hover:bg-neutral-600 disabled:opacity-50 text-fg-strong font-mono text-sm rounded px-4 py-2 transition-colors"
          >
            {loading ? 'Saving...' : 'Set password & continue'}
          </button>
        </form>
      </div>
    </div>
  )
}
