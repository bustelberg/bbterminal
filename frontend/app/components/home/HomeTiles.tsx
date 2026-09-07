'use client';

import Link from 'next/link';
import { useHomeCopy } from './homeCopy';
import { type HomeTileKey } from './homeTileKeys';

/**
 * The home page's heading and tile grid, in the reader's language.
 *
 * ⚠⚠ A CLIENT COMPONENT BECAUSE THE LANGUAGE IS A CLIENT FACT. `useLang` reads `localStorage`
 * through `useSyncExternalStore`; the server has no such preference and `i18n.ts`'s
 * `getServerSnapshot` deliberately returns `'en'` so the server HTML and the first client render
 * agree. The page itself must stay a SERVER component — it reads the session and the `view_as`
 * cookie to decide the role — so the split is: server decides WHICH tiles, client decides what
 * they SAY.
 *
 * ⚠ IT RECEIVES HREFS, NOT COPY. Passing rendered strings down would put the English in the server
 * component again and the switch would move nothing; passing the keys lets this component look them
 * up per language. It also keeps the role filter honest — the server filters the same `HomeTileKey`
 * values `isUserAllowedPath` is given, so a tile can never be advertised for a page the gate
 * refuses.
 */
export default function HomeTiles({ hrefs }: { hrefs: readonly HomeTileKey[] }) {
  const t = useHomeCopy();

  return (
    <div className="px-8 py-8 max-w-6xl">
      <div className="mb-8">
        <h1 className="text-2xl font-semibold text-fg-strong mb-2">{t.welcome}</h1>
        <p className="text-sm text-fg-muted leading-relaxed">{t.intro}</p>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {hrefs.map((href) => {
          const tile = t.tiles[href];
          return (
            <Link
              key={href}
              href={href}
              className="group block bg-card rounded-xl border border-neutral-800/40 p-5 hover:border-accent-500/40 hover:bg-inset transition-colors"
            >
              <div className="flex items-start justify-between gap-3 mb-2">
                <h2 className="text-base font-semibold text-fg-strong group-hover:text-accent-400 transition-colors">
                  {tile.label}
                </h2>
              </div>
              <p className="text-sm text-fg-muted leading-relaxed">
                {tile.description}
              </p>
            </Link>
          );
        })}
      </div>
    </div>
  );
}
