'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import { createClient } from '../../lib/supabase/client'

export default function SetPasswordPage() {
  const router = useRouter()
  const supabase = createClient()

  const [password, setPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

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
      setError(error.message)
      setLoading(false)
    } else {
      router.push('/longequity')
      router.refresh()
    }
  }

  return (
    <div className="min-h-screen bg-black flex items-center justify-center">
      <div className="w-full max-w-sm border border-gray-800 rounded p-8">
        <h1 className="font-mono text-base font-bold text-white mb-1">BBTerminal</h1>
        <p className="font-mono text-xs text-gray-500 mb-6">
          Choose a password for your account
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block font-mono text-xs text-gray-400 mb-1">Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              autoComplete="new-password"
              className="w-full bg-gray-900 border border-gray-700 rounded px-3 py-2 text-sm font-mono text-white placeholder-gray-600 focus:outline-none focus:border-gray-500"
              placeholder="Min. 8 characters"
            />
          </div>
          <div>
            <label className="block font-mono text-xs text-gray-400 mb-1">
              Confirm password
            </label>
            <input
              type="password"
              value={confirm}
              onChange={(e) => setConfirm(e.target.value)}
              required
              autoComplete="new-password"
              className="w-full bg-gray-900 border border-gray-700 rounded px-3 py-2 text-sm font-mono text-white placeholder-gray-600 focus:outline-none focus:border-gray-500"
              placeholder="••••••••"
            />
          </div>

          {error && <p className="font-mono text-xs text-red-400">{error}</p>}

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-gray-700 hover:bg-gray-600 disabled:opacity-50 text-white font-mono text-sm rounded px-4 py-2 transition-colors"
          >
            {loading ? 'Saving...' : 'Set password & continue'}
          </button>
        </form>
      </div>
    </div>
  )
}
