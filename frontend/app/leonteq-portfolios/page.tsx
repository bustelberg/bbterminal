'use client';

import { useLayoutEffect, useRef, useState } from 'react';
import { useLang } from '../../lib/i18n';
import LeonteqPanel from './LeonteqPanel';
import AirsModelsPanel from './AirsModelsPanel';
import PortfolioLinksPanel from './PortfolioLinksPanel';
import RebalancePanel from './RebalancePanel';
import ReturnsPanel from './ReturnsPanel';
import PricesPanel from './PricesPanel';
import BenchmarkPerformancePanel from './BenchmarkPerformancePanel';

const tabs = ['leonteq', 'airs', 'dynamic'] as const;
const sections = ['portfolios', 'connections', 'insights'] as const;
const insights = ['rebalance', 'returns', 'prices', 'performance'] as const;
type Tab = typeof tabs[number] | typeof sections[number] | typeof insights[number];

export default function LeonteqPortfoliosPage() {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [tab, setTab] = useState<Tab>('leonteq');
  const [section, setSection] = useState<typeof sections[number]>('portfolios');
  const [source, setSource] = useState<typeof tabs[number]>('leonteq');
  const [insight, setInsight] = useState<typeof insights[number]>('rebalance');
  const [airsOpened, setAirsOpened] = useState(false);
  const [dynamicOpened, setDynamicOpened] = useState(false);
  const [connectionsOpened, setConnectionsOpened] = useState(false);
  const [rebalanceOpened, setRebalanceOpened] = useState(false);
  const [returnsOpened, setReturnsOpened] = useState(false);
  const [pricesOpened, setPricesOpened] = useState(false);
  const [performanceOpened, setPerformanceOpened] = useState(false);
  const content = useRef<HTMLDivElement>(null);
  useLayoutEffect(() => {
    // A shorter tab must not inherit the previous view's scroll offset.
    if (content.current) content.current.scrollTop = 0;
  }, [tab]);
  function select(next: Tab) {
    if (next === 'insights') {
      select(insight);
      return;
    }
    if (next === 'portfolios') {
      setSection('portfolios');
      setTab(source);
      return;
    }
    if (next === 'leonteq' || next === 'airs' || next === 'dynamic') {
      setSource(next);
      setSection('portfolios');
    } else if (next === 'rebalance' || next === 'returns' || next === 'prices' || next === 'performance') {
      setInsight(next);
      setSection('insights');
    } else {
      setSection(next);
    }
    if (next === 'airs') setAirsOpened(true);
    if (next === 'dynamic') setDynamicOpened(true);
    if (next === 'connections') setConnectionsOpened(true);
    if (next === 'rebalance') setRebalanceOpened(true);
    if (next === 'returns') setReturnsOpened(true);
    if (next === 'prices') setPricesOpened(true);
    if (next === 'performance') setPerformanceOpened(true);
    setTab(next);
  }
  return <main className="mx-auto flex h-full min-h-0 min-w-0 w-full max-w-7xl flex-col gap-6 overflow-clip p-4 md:p-8">
    <header className="shrink-0">
      <h1 className="text-2xl font-semibold">Portfolios</h1>
    </header>
    <div className="shrink-0 bg-page py-2">
    <div role="tablist" aria-label={nl ? 'Portefeuillepagina' : 'Portfolio page'}
      className="inline-flex max-w-full overflow-x-auto rounded-lg border border-neutral-800/40 text-xs">
      {sections.map((key, index) => <button key={key} type="button" role="tab" id={`section-tab-${key}`}
        aria-selected={section === key} aria-controls={key === 'portfolios' ? 'source-panel-portfolios' : `source-panel-${key}`} tabIndex={section === key ? 0 : -1}
        onClick={() => select(key)} onKeyDown={(event) => {
          const target = event.key === 'ArrowRight' ? (index + 1) % sections.length
            : event.key === 'ArrowLeft' ? (index + sections.length - 1) % sections.length
              : event.key === 'Home' ? 0 : event.key === 'End' ? sections.length - 1 : null;
          if (target !== null) {
            event.preventDefault(); select(sections[target]);
            document.getElementById(`section-tab-${sections[target]}`)?.focus({ preventScroll: true });
          }
        }} className={`shrink-0 px-3 py-1.5 font-medium transition-colors ${section === key ? 'bg-accent-600 text-white' : 'text-fg-subtle hover:bg-overlay/5'}`}>
        {key === 'portfolios' ? 'Portfolios' : key === 'connections' ? (nl ? 'Koppelingen' : 'Connections') : 'Insights'}
      </button>)}
    </div>
    </div>
    {/* One scroll owner. Panels keep their natural content height; hidden panes
        stay mounted without reserving a viewport of empty scrollable space. */}
    <div ref={content} role="region" aria-label={nl ? 'Portefeuille-inhoud' : 'Portfolio content'} tabIndex={0}
      className="min-h-0 min-w-0 flex-1 overflow-auto overscroll-contain [overflow-anchor:none] [scrollbar-gutter:stable]">
    <div role="tabpanel" id="source-panel-portfolios" aria-labelledby="section-tab-portfolios" hidden={section !== 'portfolios'}>
    <div className="mb-4">
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
        {key === 'leonteq' ? 'Leonteq' : key === 'airs' ? 'AIRS Fixed' : 'AIRS Dynamic'}
      </button>)}
    </div>
    </div>
    <div role="tabpanel" id="source-panel-leonteq" aria-labelledby="source-tab-leonteq" hidden={tab !== 'leonteq'}><LeonteqPanel /></div>
    <div role="tabpanel" id="source-panel-airs" aria-labelledby="source-tab-airs" hidden={tab !== 'airs'}>{airsOpened && <AirsModelsPanel />}</div>
    <div role="tabpanel" id="source-panel-dynamic" aria-labelledby="source-tab-dynamic" hidden={tab !== 'dynamic'}>{dynamicOpened && <AirsModelsPanel kind="dynamic" />}</div>
    </div>
    <div role="tabpanel" id="source-panel-connections" aria-labelledby="section-tab-connections" hidden={tab !== 'connections'}>
      {connectionsOpened && <PortfolioLinksPanel active={section === 'connections'} />}
    </div>
    <div role="tabpanel" id="source-panel-insights" aria-labelledby="section-tab-insights" hidden={section !== 'insights'}>
    <div role="tablist" aria-label="Insights" className="mb-4 inline-flex max-w-full overflow-x-auto rounded-lg border border-neutral-800/40 text-xs">
      {insights.map((key, index) => <button key={key} type="button" role="tab" id={`insight-tab-${key}`}
        aria-selected={insight === key} aria-controls={`source-panel-${key}`} tabIndex={insight === key ? 0 : -1}
        onClick={() => select(key)} onKeyDown={(event) => {
          const target = event.key === 'ArrowRight' ? (index + 1) % insights.length
            : event.key === 'ArrowLeft' ? (index + insights.length - 1) % insights.length
              : event.key === 'Home' ? 0 : event.key === 'End' ? insights.length - 1 : null;
          if (target !== null) {
            event.preventDefault(); select(insights[target]);
            document.getElementById(`insight-tab-${insights[target]}`)?.focus({ preventScroll: true });
          }
        }} className={`shrink-0 px-3 py-1.5 font-medium transition-colors ${insight === key ? 'bg-accent-600 text-white' : 'text-fg-subtle hover:bg-overlay/5'}`}>
        {key === 'rebalance' ? 'Rebalance' : key === 'returns' ? 'Returns' : key === 'prices' ? 'Prices' : 'Monthly performance'}
      </button>)}
    </div>
    <div role="tabpanel" id="source-panel-rebalance" aria-labelledby="insight-tab-rebalance" hidden={tab !== 'rebalance'}>
      {rebalanceOpened && <RebalancePanel active={tab === 'rebalance'} />}
    </div>
    <div role="tabpanel" id="source-panel-returns" aria-labelledby="insight-tab-returns" hidden={tab !== 'returns'}>{returnsOpened && <ReturnsPanel />}</div>
    <div role="tabpanel" id="source-panel-prices" aria-labelledby="insight-tab-prices" hidden={tab !== 'prices'}>{pricesOpened && <PricesPanel />}</div>
    <div role="tabpanel" id="source-panel-performance" aria-labelledby="insight-tab-performance" hidden={tab !== 'performance'}>{performanceOpened && <BenchmarkPerformancePanel />}</div>
    </div>
    </div>
  </main>;
}
