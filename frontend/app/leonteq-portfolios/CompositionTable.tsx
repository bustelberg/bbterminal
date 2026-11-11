import { Fragment } from 'react';

type Position = {
  name: string; isin: string | null; total_value: string | null; units: string | null;
  price: string | null; currency: string | null; performance: string | null; weight: string | null;
  price_date?: string | null;
};
export type CompositionResult = {
  success: boolean; status: string; portfolio_name?: string; retrieved_at?: string;
  cached?: boolean; snapshot_saved?: boolean; refresh_error?: string;
  total_value?: string; total_weight?: string;
  groups?: { name: string; weight: string | null; positions: Position[] }[];
};

export default function CompositionTable({ data, nl }: { data: CompositionResult; nl: boolean }) {
  return <>
    <div className="flex flex-wrap justify-between gap-3 text-sm">
      <p className="text-fg-muted">{data.cached
        ? (nl ? 'Opgeslagen samenstelling · Vernieuwen haalt de nieuwste posities op' : 'Saved composition · Refresh retrieves the latest positions')
        : (nl ? 'Samenstelling opgehaald uit LYNQS' : 'Composition retrieved from LYNQS')}</p>
      {data.retrieved_at && <p className="text-xs text-fg-muted">{nl ? 'Opgehaald op' : 'Retrieved at'} {new Date(data.retrieved_at).toLocaleString(nl ? 'nl-NL' : 'en-GB')}</p>}
    </div>
    <div className="overflow-x-auto rounded-xl border border-neutral-800/40">
      <table className="w-full whitespace-nowrap text-sm">
        <caption className="sr-only">{data.portfolio_name} · {nl ? 'Samenstelling' : 'Composition'}</caption>
        <thead className="bg-elevated text-xs text-fg-muted"><tr>
          {[nl ? 'Instrumentnaam / ISIN' : 'Instrument name / ISIN', nl ? 'Totale waarde / exposure' : 'Total value / exposure', nl ? 'Aantal' : 'Total units', nl ? 'Huidige koers' : 'Current price', 'CCY', nl ? 'Gem. rendement' : 'Avg. performance', nl ? 'Gewicht' : 'Weight'].map((label, index) =>
            <th scope="col" key={label} className={`px-4 py-3 font-medium ${index ? 'text-right' : 'text-left'}`}>{label}</th>)}
        </tr></thead>
        <tbody className="divide-y divide-neutral-800/30">
          {data.groups?.map((group, groupIndex) => <Fragment key={`${group.name}-${groupIndex}`}>
            <tr className="bg-elevated/70 font-semibold"><th scope="row" colSpan={6} className="px-4 py-3 text-left">{group.name}</th><td className="px-4 py-3 text-right tabular-nums">{group.weight ?? '—'}</td></tr>
            {group.positions.map((position, index) => <tr key={`${position.isin ?? position.name}-${index}`} className="hover:bg-elevated/40">
              <th scope="row" className="px-4 py-3 text-left font-medium"><div>{position.name}</div>{position.isin && <div className="mt-1 text-xs font-normal text-fg-muted">{position.isin}</div>}</th>
              <td className="px-4 py-3 text-right tabular-nums">{position.total_value ?? '—'}</td>
              <td className="px-4 py-3 text-right tabular-nums">{position.units ?? '—'}</td>
              <td className="px-4 py-3 text-right tabular-nums">{position.price ?? '—'}</td>
              <td className="px-4 py-3 text-right">{position.currency ?? '—'}</td>
              <td className={`px-4 py-3 text-right tabular-nums ${position.performance?.startsWith('-') && position.performance !== '-' ? 'text-neg-300' : Number.parseFloat(position.performance ?? '') > 0 ? 'text-pos-300' : 'text-fg-muted'}`}>{position.performance ?? '—'}</td>
              <td className="px-4 py-3 text-right tabular-nums">{position.weight ?? '—'}</td>
            </tr>)}
          </Fragment>)}
        </tbody>
        <tfoot className="border-t border-neutral-800/40 bg-elevated font-semibold"><tr>
          <th scope="row" className="px-4 py-4 text-left">{nl ? 'Totale waarde' : 'Total value'}</th>
          <td className="px-4 py-4 text-right tabular-nums">{data.total_value ?? '—'}</td>
          <td colSpan={4} className="px-4 py-4 text-right">{nl ? 'Totaal gewicht' : 'Total weight'}</td>
          <td className="px-4 py-4 text-right tabular-nums">{data.total_weight ?? '—'}</td>
        </tr></tfoot>
      </table>
    </div>
  </>;
}
