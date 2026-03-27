'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import { createClient } from '../../lib/supabase/client'

const ALLOWED_DOMAIN = 'bustelberg.nl'
const ALLOWED_EMAILS = (process.env.NEXT_PUBLIC_ALLOWED_EMAILS ?? '')
  .split(',')
  .map((e) => e.trim().toLowerCase())
  .filter(Boolean)

function isAllowed(email: string): boolean {
  const lower = email.toLowerCase()
  return lower.endsWith(`@${ALLOWED_DOMAIN}`) || ALLOWED_EMAILS.includes(lower)
}

export default function LoginPage() {
  const router = useRouter()
  const supabase = createClient()

  const [mode, setMode] = useState<'signin' | 'signup'>('signin')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [info, setInfo] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError(null)
    setInfo(null)

    if (!isAllowed(email)) {
      setError(`Access is restricted to @${ALLOWED_DOMAIN} accounts.`)
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
        setError(error.message)
      } else {
        setInfo('Check your email for a confirmation link. After clicking it you\'ll set your password.')
      }
    } else {
      const { error } = await supabase.auth.signInWithPassword({ email, password })
      if (error) {
        setError(error.message)
      } else {
        router.push('/longequity')
        router.refresh()
      }
    }

    setLoading(false)
  }

  return (
    <div className="min-h-screen bg-black flex items-center justify-center">
      <div className="w-full max-w-sm border border-gray-800 rounded p-8">
        <h1 className="font-mono text-base font-bold text-white mb-1">BBTerminal</h1>
        <p className="font-mono text-xs text-gray-500 mb-6">
          {mode === 'signin' ? 'Sign in to your account' : 'Request access'}
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block font-mono text-xs text-gray-400 mb-1">Email</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
              autoComplete="email"
              className="w-full bg-gray-900 border border-gray-700 rounded px-3 py-2 text-sm font-mono text-white placeholder-gray-600 focus:outline-none focus:border-gray-500"
              placeholder="you@bustelberg.nl"
            />
          </div>

          {mode === 'signin' && (
            <div>
              <label className="block font-mono text-xs text-gray-400 mb-1">Password</label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                autoComplete="current-password"
                className="w-full bg-gray-900 border border-gray-700 rounded px-3 py-2 text-sm font-mono text-white placeholder-gray-600 focus:outline-none focus:border-gray-500"
                placeholder="••••••••"
              />
            </div>
          )}

          {error && <p className="font-mono text-xs text-red-400">{error}</p>}
          {info  && <p className="font-mono text-xs text-green-400">{info}</p>}

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-gray-700 hover:bg-gray-600 disabled:opacity-50 text-white font-mono text-sm rounded px-4 py-2 transition-colors"
          >
            {loading
              ? 'Please wait...'
              : mode === 'signin'
              ? 'Sign in'
              : 'Send confirmation link'}
          </button>
        </form>

        <p className="mt-4 font-mono text-xs text-gray-600 text-center">
          {mode === 'signin' ? 'New user?' : 'Already have an account?'}{' '}
          <button
            onClick={() => {
              setMode(mode === 'signin' ? 'signup' : 'signin')
              setError(null)
              setInfo(null)
            }}
            className="text-gray-400 hover:text-white underline"
          >
            {mode === 'signin' ? 'Request access' : 'Sign in'}
          </button>
        </p>
      </div>
    </div>
  )
}
