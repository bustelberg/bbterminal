'use client';

import { useHomeCopy } from './homeCopy';

/** Minimal home welcome; navigation lives in the sidebar. */
export default function HomeWelcome() {
  const t = useHomeCopy();
  return (
    <div className="px-8 py-8 max-w-6xl">
      <h1 className="text-2xl font-semibold text-fg-strong">{t.welcome}</h1>
    </div>
  );
}
