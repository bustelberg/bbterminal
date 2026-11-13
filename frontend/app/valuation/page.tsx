'use client';

import ValuationPanel from '../components/management/ValuationPanel';
import { useSidebarCopy } from '../components/sidebarCopy';

export default function ValuationPage() {
  const t = useSidebarCopy();
  return <main className="mx-auto w-full max-w-7xl space-y-5 p-4 text-fg md:p-8">
    <h1 className="text-xl font-semibold text-fg-strong">{t.nav['/valuation']}</h1>
    <ValuationPanel />
  </main>;
}
