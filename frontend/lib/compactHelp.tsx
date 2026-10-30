'use client';

import { createContext, useContext, type ReactNode } from 'react';
import { useLang, type Lang } from './i18n';
import DynamicText from './dynamicValue';
import Formula from './formula';

const CompactHelpContext = createContext(false);

/** Dashboard only: shared dialogs and portals inherit this presentation. */
export function CompactHelpProvider({ children }: { children: ReactNode }) {
  return <CompactHelpContext.Provider value>{children}</CompactHelpContext.Provider>;
}

export const useCompactHelp = () => useContext(CompactHelpContext);

/** Keep the numerical half of withWorked's explicit two-line format, without changing it. */
export function workedExample(text?: string): string | null {
  if (!text) return null;
  const start = String.raw`\begin{gathered}`, end = String.raw`\end{gathered}`;
  const separator = String.raw`\\[4pt]`;
  if (!text.startsWith(start) || !text.endsWith(end)) return null;
  const body = text.slice(start.length, -end.length);
  const split = body.indexOf(separator);
  return split < 0 ? null : body.slice(split + separator.length).trim() || null;
}

const prose = (value: ReactNode) => typeof value === 'string' ? <DynamicText text={value} /> : value;

/** Brief explanation, checkable numbers and source dates; full notation stays optional. */
export function CompactHelp({ summary, method, facts, worked, notation, calculation, lang: suppliedLang }: {
  summary?: ReactNode; method?: ReactNode; facts?: ReactNode; worked?: string;
  notation?: ReactNode; calculation?: ReactNode; lang?: Lang;
}) {
  const [globalLang] = useLang();
  const lang = suppliedLang ?? globalLang;
  const example = workedExample(worked);
  const rule = typeof method === 'string' ? method.split(/\n\s*\n/)[0].trim() : '';
  // This selects a layout, never truncates prose: long methods remain intact in details.
  const shortRule = rule.length <= 220 && rule.split(/\s+/).length <= 40 ? rule : '';
  const extended = typeof method === 'string' && method && method.trim() !== shortRule;
  // Existing callers deliberately separate the rule and its actual operands with a blank line.
  const plainExample = typeof method === 'string' && !worked
    ? method.split(/\n\s*\n/).slice(1).join('\n\n').trim() : '';
  return <div className="min-w-[13rem] space-y-2 text-xs leading-relaxed">
    {summary && <div className="font-medium text-fg-strong">{prose(summary)}</div>}
    {shortRule && shortRule !== summary && <div>{prose(shortRule)}</div>}
    {example && <div className="overflow-x-auto rounded-md border border-neutral-800/40 bg-overlay/5 p-2"><Formula tex={example} /></div>}
    {plainExample && <div className="whitespace-pre-line font-mono tabular-nums">{prose(plainExample)}</div>}
    {/* Caller-designed tables and calculations remain visible, never flattened into prose. */}
    {typeof method !== 'string' && method}
    {calculation}
    {facts && <div className="space-y-1 text-fg-muted">{facts}</div>}
    {(extended || worked || notation) && <details className="text-fg-muted">
      <summary className="cursor-pointer text-fg-subtle">{lang === 'nl' ? 'Berekening en toelichting' : 'Calculation details'}</summary>
      <div className="mt-2 space-y-2 whitespace-pre-line">
        {typeof method === 'string' && <div>{prose(method)}</div>}
        {worked && <div className="overflow-x-auto"><Formula tex={worked} /></div>}
        {notation}
      </div>
    </details>}
  </div>;
}
