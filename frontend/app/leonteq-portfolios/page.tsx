'use client';

import { useState } from 'react';
import { useLang } from '../../lib/i18n';
import LeonteqPanel from './LeonteqPanel';
import AirsModelsPanel from './AirsModelsPanel';
import PortfolioLinksPanel from './PortfolioLinksPanel';
import RebalancePanel from './RebalancePanel';
import ReturnsPanel from './ReturnsPanel';

const tabs = ['leonteq', 'airs', 'dynamic', 'connections', 'rebalance', 'returns'] as const;
type Tab = typeof tabs[number];

export default function LeonteqPortfoliosPage() {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [tab, setTab] = useState<Tab>('leonteq');
  const [airsOpened, setAirsOpened] = useState(false);
  const [dynamicOpened, setDynamicOpened] = useState(false);
  const [connectionsOpened, setConnectionsOpened] = useState(false);
  const [rebalanceOpened, setRebalanceOpened] = useState(false);
  const [returnsOpened, setReturnsOpened] = useState(false);
  function select(next: Tab) {
    if (next === 'airs') setAirsOpened(true);
    if (next === 'dynamic') setDynamicOpened(true);
    if (next === 'connections') setConnectionsOpened(true);
    if (next === 'rebalance') setRebalanceOpened(true);
    if (next === 'returns') setReturnsOpened(true);
    setTab(next);
  }
  return <main className="mx-auto flex h-full min-h-0 min-w-0 w-full max-w-7xl flex-col gap-6 overflow-hidden p-4 md:p-8">
    <header className="shrink-0">
      <h1 className="text-2xl font-semibold">{nl ? 'Leonteq-portefeuilles' : 'Leonteq Portfolios'}</h1>
      <p className="mt-2 text-sm text-fg-muted">Leonteq · AIRS Fixed · AIRS Dynamic</p>
    </header>
    <div className="shrink-0 bg-page py-2">
    <div role="tablist" aria-label={nl ? 'Portefeuillebron' : 'Portfolio source'}
      className="inline-flex max-w-full overflow-x-auto rounded-lg border border-neutral-800/40 text-xs">
      {tabs.map((key, index) => <button key={key} type="button" role="tab" id={`source-tab-${key}`}
        aria-selected={tab === key} aria-controls={`source-panel-${key}`} tabIndex={tab === key ? 0 : -1}
        onClick={() => select(key)} onKeyDown={(event) => {
          const target = event.key === 'ArrowRight' ? (index + 1) % tabs.length
            : event.key === 'ArrowLeft' ? (index + tabs.length - 1) % tabs.length
              : event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 : null;
          if (target !== null) {
            event.preventDefault(); select(tabs[target]);
            document.getElementById(`source-tab-${tabs[target]}`)?.focus({ preventScroll: true });
          }
        }} className={`shrink-0 px-3 py-1.5 font-medium transition-colors ${tab === key ? 'bg-accent-600 text-white' : 'text-fg-subtle hover:bg-overlay/5'}`}>
        {key === 'leonteq' ? 'Leonteq' : key === 'airs' ? 'AIRS Fixed' : key === 'dynamic' ? 'AIRS Dynamic' : key === 'connections' ? (nl ? 'Koppelingen' : 'Connections') : key === 'returns' ? 'Returns' : 'Rebalance'}
      </button>)}
    </div>
    </div>
    <div className="min-h-0 min-w-0 flex-1 overflow-hidden [&>[role=tabpanel]]:h-full [&>[role=tabpanel]]:min-w-0 [&>[role=tabpanel]]:overflow-auto [&>[role=tabpanel]]:[overflow-anchor:none] [&>[role=tabpanel]]:[scrollbar-gutter:stable]">
    <div role="tabpanel" id="source-panel-leonteq" aria-labelledby="source-tab-leonteq" hidden={tab !== 'leonteq'}><LeonteqPanel /></div>
    <div role="tabpanel" id="source-panel-airs" aria-labelledby="source-tab-airs" hidden={tab !== 'airs'}>{airsOpened && <AirsModelsPanel />}</div>
    <div role="tabpanel" id="source-panel-dynamic" aria-labelledby="source-tab-dynamic" hidden={tab !== 'dynamic'}>{dynamicOpened && <AirsModelsPanel kind="dynamic" />}</div>
    <div role="tabpanel" id="source-panel-connections" aria-labelledby="source-tab-connections" hidden={tab !== 'connections'}>
      {connectionsOpened && <PortfolioLinksPanel />}
    </div>
    <div role="tabpanel" id="source-panel-rebalance" aria-labelledby="source-tab-rebalance" hidden={tab !== 'rebalance'}>
      {rebalanceOpened && <RebalancePanel active={tab === 'rebalance'} />}
    </div>
    <div role="tabpanel" id="source-panel-returns" aria-labelledby="source-tab-returns" hidden={tab !== 'returns'}>{returnsOpened && <ReturnsPanel />}</div>
    </div>
  </main>;
}
