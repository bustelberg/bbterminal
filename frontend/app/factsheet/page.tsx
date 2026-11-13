'use client';

import FactsheetPanel from '../components/management/FactsheetPanel';

export default function FactsheetPage() {
  return <main className="mx-auto w-full max-w-7xl space-y-5 p-4 text-fg md:p-8">
    <h1 className="text-xl font-semibold text-fg-strong">Factsheet</h1>
    <FactsheetPanel />
  </main>;
}
