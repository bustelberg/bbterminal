import type { ReactNode } from 'react';

type Props = {
  title: string;
  count: number;
  retrievedAt?: string | null;
  nl: boolean;
  query: string;
  onQueryChange: (query: string) => void;
  searchLabel: string;
  searchPlaceholder: string;
  refreshLabel: string;
  refreshDisabled: boolean;
  onRefresh: () => void;
  children?: ReactNode;
};

export default function PortfolioSourceHeader(props: Props) {
  return <div className="space-y-4">
    <div className="flex flex-wrap items-center justify-between gap-4">
      <div>
        <h2 className="text-lg font-semibold">{props.title} <span className="text-sm font-normal text-fg-muted">({props.count})</span></h2>
        {props.retrievedAt && <p className="mt-1 text-xs text-fg-muted">{props.nl ? 'Opgehaald op' : 'Retrieved at'} {new Date(props.retrievedAt).toLocaleString(props.nl ? 'nl-NL' : 'en-GB')}</p>}
      </div>
      <div className="flex flex-wrap items-center gap-4">
        <button type="button" disabled={props.refreshDisabled} onClick={props.onRefresh}
          className="rounded-lg bg-accent-600 px-4 py-2 text-sm font-medium text-white hover:bg-accent-500 disabled:opacity-50">{props.refreshLabel}</button>
        {props.children}
      </div>
    </div>
    <input type="search" value={props.query} onChange={(event) => props.onQueryChange(event.target.value)}
      aria-label={props.searchLabel} placeholder={props.searchPlaceholder}
      className="w-full max-w-md rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-sm" />
  </div>;
}
