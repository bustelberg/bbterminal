'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

const TABS = [
  { href: '/asset-pipeline', label: 'Execution instruments' },
  { href: '/alphalab', label: 'AlphaLab' },
  { href: '/signal-lab', label: 'Signal Lab' },
];

/** Tab nav shared by the asset-pipeline pages (Asset Pipeline home + AlphaLab). */
export default function AssetNav() {
  const path = usePathname();
  return (
    <nav className="flex items-center gap-1 px-8 pt-4 -mb-px">
      {TABS.map((t) => {
        const active = path === t.href;
        return (
          <Link
            key={t.href} href={t.href}
            className={`px-4 py-2 text-sm rounded-t-lg border-b-2 transition-colors ${
              active
                ? 'border-accent-500 text-accent-300 font-medium'
                : 'border-transparent text-fg-muted hover:text-fg-strong hover:bg-overlay/5'
            }`}
          >
            {t.label}
          </Link>
        );
      })}
    </nav>
  );
}
