'use client';

import QualityUniverse from '../components/QualityUniverse';

export default function QualityUniversePage() {
  return (
    <div className="h-full flex flex-col bg-page">
      <div className="px-8 py-5 border-b border-neutral-800/60">
        <h1 className="text-fg-strong text-xl font-semibold">Quality Universe</h1>
        <p className="text-fg-subtle text-sm mt-1">
          Combined companies from Global Compounders and iShares World Quality holdings.
        </p>
      </div>
      <div className="flex-1 overflow-auto px-8 py-5">
        <QualityUniverse />
      </div>
    </div>
  );
}
