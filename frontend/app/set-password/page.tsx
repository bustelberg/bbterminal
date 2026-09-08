'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { useRouter } from 'next/navigation'
import { createClient } from '../../lib/supabase/client'
import AuthShell, {
  AuthNotice,
  authButtonClass,
  authFieldClass,
  authLabelClass,
  authSecondaryButtonClass,
} from '../components/auth/AuthShell'

const MIN_LENGTH = 8

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
  const [reveal, setReveal] = useState(false)
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

  /**
   * ⚠⚠ THE TWO RULES ARE CHECKED WHILE TYPING, NOT ONLY AT SUBMIT. Reported from the live form:
   * fifteen characters in the first box, eight in the second, and the page said nothing until the
   * button was pressed — so the first thing this screen ever tells a new user is that they got it
   * wrong. Both rules are decidable from what is on screen, so there is no reason to wait.
   *
   * ⚠ THE MISMATCH LINE IS SUPPRESSED WHILE THE SECOND BOX IS EMPTY, and the length line while the
   * first is. "Too short" under an empty field is a complaint about not having typed yet.
   */
  const tooShort = password.length > 0 && password.length < MIN_LENGTH
  const mismatch = confirm.length > 0 && password !== confirm
  const ready = password.length >= MIN_LENGTH && password === confirm

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError(null)

    if (password !== confirm) {
      setError('The two passwords do not match.')
      return
    }
    if (password.length < MIN_LENGTH) {
      setError(`Password must be at least ${MIN_LENGTH} characters.`)
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
      <AuthShell title="One moment" subtitle="Checking your sign-in link…">
        <div className="flex items-center gap-3 text-sm text-fg-subtle">
          <span
            aria-hidden
            className="h-4 w-4 shrink-0 rounded-full border-2 border-accent-300 border-t-accent-600 animate-spin"
          />
          This only takes a second.
        </div>
      </AuthShell>
    )
  }

  // ⚠ NO PASSWORD FORM WITHOUT A SESSION. Offering one would be offering an action that cannot
  // succeed — and the failure would arrive after the work, phrased as a fault in the password.
  if (!signedIn) {
    return (
      <AuthShell
        title="This link is no longer active"
        subtitle="There is no account signed in, so there is nothing to set a password on yet."
      >
        <div className="space-y-4">
          <AuthNotice kind="error">
            Sign-in links work once, expire after an hour, and have to be opened in the same
            browser you asked for them from.
          </AuthNotice>
          <Link href="/login" className={authSecondaryButtonClass}>
            Request a new link
          </Link>
        </div>
      </AuthShell>
    )
  }

  return (
    <AuthShell
      title="Choose a password"
      subtitle="Last step — pick something you will remember, then you are in."
      footer={`At least ${MIN_LENGTH} characters. You can change it later from your account menu.`}
    >
      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <div className="flex items-baseline justify-between mb-1.5">
            {/* ⚠ NOT `${authLabelClass} mb-0`. Tailwind resolves a conflict by stylesheet order,
                not by position in the class string, so appending an override is a coin toss —
                this row carries the spacing itself and the label is spelled out. */}
            <label htmlFor="password" className="block text-xs font-medium text-fg-muted">
              Password
            </label>
            {/* ⚠ ONE TOGGLE FOR BOTH FIELDS. Two would let someone reveal the box they typed
                correctly and keep the other hidden, which is the opposite of what it is for. */}
            <button
              type="button"
              onClick={() => setReveal((v) => !v)}
              className="text-xs text-accent-400 hover:text-accent-500 transition-colors"
            >
              {reveal ? 'Hide' : 'Show'}
            </button>
          </div>
          <input
            id="password"
            type={reveal ? 'text' : 'password'}
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
            autoComplete="new-password"
            autoFocus
            aria-invalid={tooShort || undefined}
            className={authFieldClass}
            placeholder={`Min. ${MIN_LENGTH} characters`}
          />
          {tooShort && (
            <p className="mt-1.5 text-xs text-fg-subtle">
              {MIN_LENGTH - password.length} more character{MIN_LENGTH - password.length === 1 ? '' : 's'} to go.
            </p>
          )}
        </div>

        <div>
          <label htmlFor="confirm" className={authLabelClass}>Confirm password</label>
          <input
            id="confirm"
            type={reveal ? 'text' : 'password'}
            value={confirm}
            onChange={(e) => setConfirm(e.target.value)}
            required
            autoComplete="new-password"
            aria-invalid={mismatch || undefined}
            className={`${authFieldClass} ${mismatch ? 'border-neg-300 focus:border-neg-400 focus:ring-neg-400/25' : ''}`}
            placeholder="••••••••"
          />
          {mismatch && <p className="mt-1.5 text-xs text-neg-400">The two passwords do not match yet.</p>}
          {ready && <p className="mt-1.5 text-xs text-pos-400">Passwords match.</p>}
        </div>

        {error && <AuthNotice kind="error">{error}</AuthNotice>}

        <button type="submit" disabled={loading} className={authButtonClass}>
          {loading ? 'Saving…' : 'Set password & continue'}
        </button>
      </form>
    </AuthShell>
  )
}
