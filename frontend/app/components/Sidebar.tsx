'use client';

import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import { createClient } from '../../lib/supabase/client';

const navItems = [
  { href: '/longequity', label: 'LongEquity Insight' },
  { href: '/airs-portfolio', label: 'AIRS Portfolio' },
  { href: '/companies', label: 'Companies' },
];

const AUTH_PAGES = ['/login', '/set-password'];

export default function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();
  const [email, setEmail] = useState<string | null>(null);
  const [checked, setChecked] = useState(false);

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
              pathname.startsWith(href)
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
      </div>
    </aside>
  );
}
