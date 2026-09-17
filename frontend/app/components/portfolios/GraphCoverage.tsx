'use client';

import { createContext, type ReactNode, useContext } from 'react';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';

/** One shared, always-visible coverage statement for every card on Graphs. */
export type GraphCoverageValue = {
  text: string;
  what: string;
  where: string;
  when: string;
  how: string;
};

const Context = createContext<GraphCoverageValue | null>(null);

export const useGraphCoverage = () => useContext(Context);

export function GraphCoverageProvider({ value, children }: {
  value: GraphCoverageValue | null;
  children: ReactNode;
}) {
  return <Context.Provider value={value}>{children}</Context.Provider>;
}

/** Rendered by CardHeading, so a new Graphs card cannot accidentally omit the disclosure. */
export function GraphCoverageLine() {
  const value = useGraphCoverage();
  if (!value) return null;
  return (
    <p className="text-[11px] font-normal text-fg-faint mt-0.5 whitespace-normal">
      {value.text}
      <InfoTip className="ml-1" content={<AspectCard
        what={value.what} where={value.where} when={value.when} how={value.how} />} />
    </p>
  );
}
