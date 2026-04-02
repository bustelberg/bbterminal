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
        router.push('/')
        router.refresh()
      }
    }

    setLoading(false)
  }

  return (
    <div className="min-h-screen bg-[#0f1117] flex items-center justify-center">
      <div className="w-full max-w-sm bg-[#151821] border border-gray-800/40 rounded-xl p-8">
        <h1 className="text-lg font-semibold text-white mb-1">BBTerminal</h1>
        <p className="text-sm text-gray-500 mb-6">
          {mode === 'signin' ? 'Sign in to your account' : 'Request access'}
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-xs font-medium text-gray-400 mb-1.5">Email</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
              autoComplete="email"
              className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2.5 text-sm text-white placeholder-gray-600 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors"
              placeholder="you@bustelberg.nl"
            />
          </div>

          {mode === 'signin' && (
            <div>
              <label className="block text-xs font-medium text-gray-400 mb-1.5">Password</label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                autoComplete="current-password"
                className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2.5 text-sm text-white placeholder-gray-600 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors"
                placeholder="••••••••"
              />
            </div>
          )}

          {error && <p className="text-xs text-rose-400">{error}</p>}
          {info  && <p className="text-xs text-emerald-400">{info}</p>}

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white text-sm font-medium rounded-lg px-4 py-2.5 transition-colors"
          >
            {loading
              ? 'Please wait...'
              : mode === 'signin'
              ? 'Sign in'
              : 'Send confirmation link'}
          </button>
        </form>

        <p className="mt-5 text-xs text-gray-600 text-center">
          {mode === 'signin' ? 'New user?' : 'Already have an account?'}{' '}
          <button
            onClick={() => {
              setMode(mode === 'signin' ? 'signup' : 'signin')
              setError(null)
              setInfo(null)
            }}
            className="text-indigo-400 hover:text-indigo-300 transition-colors"
          >
            {mode === 'signin' ? 'Request access' : 'Sign in'}
          </button>
        </p>
      </div>
    </div>
  )
}
