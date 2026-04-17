'use client';

import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import { createClient } from '../../lib/supabase/client';

const navItems = [
  { href: '/longequity', label: 'LongEquity Insight' },
  { href: '/earnings', label: 'Earnings Dashboard' },
  { href: '/momentum', label: 'Momentum' },
  { href: '/universe', label: 'Universe' },
  { href: '/universe_index', label: 'Index Universe' },
  { href: '/acwi', label: 'ACWI Universe' },
  { href: '/airs-portfolio', label: 'AIRS Portfolio' },
  { href: '/benchmarks', label: 'Benchmarks' },
  { href: '/companies', label: 'Companies' },
];

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';
const AUTH_PAGES = ['/login', '/set-password'];

export default function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();
  const [email, setEmail] = useState<string | null>(null);
  const [checked, setChecked] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [deleting, setDeleting] = useState(false);

  useEffect(() => {
    const supabase = createClient();
    supabase.auth.getUser().then(({ data }) => {
      setEmail(data.user?.email ?? null);
      setChecked(true);
    });
  }, []);

  async function handleSignOut() {
    const supabase = createClient();
    await supabase.auth.signOut();
    router.push('/login');
    router.refresh();
  }

  async function handleDeleteAccount() {
    setDeleting(true);
    try {
      const supabase = createClient();
      const { data: { session } } = await supabase.auth.getSession();
      if (!session) throw new Error('No session');
      const res = await fetch(`${API_URL}/api/auth/delete-account`, {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${session.access_token}` },
      });
      if (!res.ok) {
        const body = await res.text();
        throw new Error(`${res.status}: ${body}`);
      }
      await supabase.auth.signOut();
      router.push('/login');
      router.refresh();
    } catch (err) {
      alert(`Failed to delete account:\n${err instanceof Error ? err.message : err}`);
    } finally {
      setDeleting(false);
      setShowDeleteConfirm(false);
    }
  }

  // Hide sidebar on auth pages or when not logged in
  if (!checked || !email || AUTH_PAGES.includes(pathname)) return null;

  return (
    <aside className="w-56 shrink-0 border-r border-gray-800/60 bg-[#0b0d13] flex flex-col">
      <div className="px-5 py-5 border-b border-gray-800/60">
        <Link href="/" className="text-lg font-semibold tracking-tight text-white hover:text-gray-300 transition-colors">
          BBTerminal
        </Link>
      </div>
      <nav className="flex-1 p-3 space-y-1">
        {navItems.map(({ href, label }) => (
          <Link
            key={href}
            href={href}
            className={`block px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
              pathname === href || (pathname.startsWith(href + '/') && href !== '/')
                ? 'bg-indigo-600/15 text-indigo-400'
                : 'text-gray-400 hover:text-white hover:bg-white/5'
            }`}
          >
            {label}
          </Link>
        ))}
      </nav>
      <div className="p-3 border-t border-gray-800/60 space-y-1">
        {email && (
          <p className="px-3 py-1.5 text-sm text-gray-500 truncate" title={email}>
            {email}
          </p>
        )}
        <button
          onClick={handleSignOut}
          className="w-full px-3 py-2.5 rounded-lg text-sm font-medium text-gray-500 hover:text-white hover:bg-white/5 transition-colors text-left"
        >
          Sign out
        </button>
        {!showDeleteConfirm ? (
          <button
            onClick={() => setShowDeleteConfirm(true)}
            className="w-full px-3 py-2.5 rounded-lg text-sm font-medium text-gray-500 hover:text-rose-400 hover:bg-rose-500/10 transition-colors text-left"
          >
            Delete account
          </button>
        ) : (
          <div className="px-3 py-2 space-y-2">
            <p className="text-sm text-rose-400">Are you sure? This cannot be undone.</p>
            <div className="flex gap-2">
              <button
                onClick={handleDeleteAccount}
                disabled={deleting}
                className="flex-1 px-2 py-1.5 rounded-lg text-sm font-medium bg-rose-600 hover:bg-rose-500 text-white transition-colors disabled:opacity-50"
              >
                {deleting ? 'Deleting...' : 'Yes, delete'}
              </button>
              <button
                onClick={() => setShowDeleteConfirm(false)}
                className="flex-1 px-2 py-1.5 rounded-lg text-sm font-medium text-gray-400 hover:text-white hover:bg-white/5 transition-colors"
              >
                Cancel
              </button>
            </div>
          </div>
        )}
      </div>
    </aside>
  );
}
