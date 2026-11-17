'use client';

import ValuationPanel from '../components/management/ValuationPanel';
import { useSidebarCopy } from '../components/sidebarCopy';

export default function ValuationPage() {
  const t = useSidebarCopy();
  return <main className="mx-auto w-[90%] max-w-none space-y-5 py-4 text-fg md:py-8">
    <h1 className="text-xl font-semibold text-fg-strong">{t.nav['/valuation']}</h1>
    <ValuationPanel />
  </main>;
}
