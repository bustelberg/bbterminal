'use client';

import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import { createClient } from '../../lib/supabase/client';

const navItems = [
  { href: '/longequity', label: 'LongEquity Insight' },
];

export default function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();
  const [email, setEmail] = useState<string | null>(null);

  useEffect(() => {
    const supabase = createClient();
    supabase.auth.getUser().then(({ data }) => {
      setEmail(data.user?.email ?? null);
    });
  }, []);

  async function handleSignOut() {
    const supabase = createClient();
    await supabase.auth.signOut();
    router.push('/login');
    router.refresh();
  }

  return (
    <aside className="w-52 shrink-0 border-r border-gray-800 flex flex-col">
      <div className="px-4 py-3 border-b border-gray-800">
        <span className="font-mono text-sm font-bold tracking-wide text-white">
          BBTerminal
        </span>
      </div>
      <nav className="flex-1 p-2 space-y-0.5">
        {navItems.map(({ href, label }) => (
          <Link
            key={href}
            href={href}
            className={`block px-3 py-2 rounded text-xs font-mono transition-colors ${
              pathname.startsWith(href)
                ? 'bg-gray-700 text-white'
                : 'text-gray-400 hover:text-white hover:bg-gray-800'
            }`}
          >
            {label}
          </Link>
        ))}
      </nav>
      <div className="p-2 border-t border-gray-800 space-y-1">
        {email && (
          <p className="px-3 py-1 text-xs font-mono text-gray-600 truncate" title={email}>
            {email}
          </p>
        )}
        <button
          onClick={handleSignOut}
          className="w-full px-3 py-2 rounded text-xs font-mono text-gray-500 hover:text-white hover:bg-gray-800 transition-colors text-left"
        >
          Sign out
        </button>
      </div>
    </aside>
  );
}
