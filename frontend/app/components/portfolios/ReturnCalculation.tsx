'use client';

import { useLang } from '../../../lib/i18n';

export type ReturnCalculationRow = {
  name?: string | null;
  ticker?: string | null;
  isin?: string | null;
  weight_pct?: number | null;
  return_pct?: number | null;
  return_currency?: string | null;
  return_start_date?: string | null;
  return_start_price_local?: number | null;
  return_start_price_eur?: number | null;
  return_end_date?: string | null;
  return_end_price_local?: number | null;
  return_end_price_eur?: number | null;
};

const percent = (value: number, signed = false) =>
  `${signed && value >= 0 ? '+' : ''}${value.toFixed(2)}%`;
const points = (value: number) => `${value >= 0 ? '+' : ''}${value.toFixed(2)}pp`;
const price = (value: number, currency: string) => `${value.toFixed(2)} ${currency}`;

export function returnCalculation(rows: ReturnCalculationRow[]) {
  const priced = rows.filter((row) => row.weight_pct != null && row.weight_pct > 0
    && row.return_pct != null);
  const denominator = priced.reduce((sum, row) => sum + row.weight_pct!, 0);
  return {
    denominator,
    rows: denominator > 0
      ? priced.map((row) => ({ row, contribution: row.weight_pct! / denominator * row.return_pct! }))
      : [],
  };
}

/** The literal holding weights and returns behind one bucket's weighted return. */
export default function ReturnCalculation({ rows, result, bucket, owner }: {
  rows: ReturnCalculationRow[];
  result: string;
  bucket: string;
  owner: string;
}) {
  const [lang] = useLang();
  const calculation = returnCalculation(rows);
  const { denominator } = calculation;
  if (denominator <= 0) return null;

  const copy = lang === 'nl' ? {
    heading: 'Werkelijke waarden in de berekening',
    intro: `${owner} in ${bucket}: iedere positie draagt naar verhouding van haar openingsgewicht bij.`,
    sum: `Som van ${calculation.rows.length} bijdragen aan het rendement`,
    holding: 'Positie', weight: 'Openingsgewicht', native: 'Koers lokaal', eur: 'Koers EUR', ret: 'EUR-rend.', contribution: 'Bijdrage',
  } : {
    heading: 'Actual values in the calculation',
    intro: `${owner} in ${bucket}: each holding contributes in proportion to its opening weight.`,
    sum: `Sum of ${calculation.rows.length} contributions to the return`,
    holding: 'Holding', weight: 'Opening weight', native: 'Local price', eur: 'EUR price', ret: 'EUR return', contribution: 'Contribution',
  };

  return (
    <div className="rounded-md border border-neutral-800/40 bg-overlay/[0.05] px-2.5 py-2">
      <span className="block text-[10px] uppercase tracking-wider text-fg-faint mb-1">
        {copy.heading}
      </span>
      <span className="block text-[11px] leading-relaxed text-fg-muted mb-1.5">
        {copy.intro}
      </span>
      <div className="max-h-64 overflow-auto rounded border border-neutral-800/30">
        <table className="w-full min-w-[680px] text-[10px] tabular-nums">
          <thead className="sticky top-0 bg-inset/95 text-fg-faint">
            <tr className="text-left">
              <th className="px-2 py-1 font-medium">{copy.holding}</th>
              <th className="px-2 py-1 text-right font-medium">{copy.weight}</th>
              <th className="px-2 py-1 font-medium">{copy.native}</th>
              <th className="px-2 py-1 font-medium">{copy.eur}</th>
              <th className="px-2 py-1 text-right font-medium">{copy.ret}</th>
              <th className="px-2 py-1 text-right font-medium">{copy.contribution}</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
          {calculation.rows.map(({ row, contribution }, index) => {
          const label = row.name ?? row.ticker ?? row.isin ?? `#${index + 1}`;
          const hasNativePrices = row.return_currency != null
            && row.return_start_price_local != null && row.return_end_price_local != null;
          const hasEurPrices = row.return_start_price_eur != null && row.return_end_price_eur != null;
          return (
            <tr key={`${row.isin ?? row.ticker ?? label}-${index}`}>
              <td className="max-w-[150px] truncate px-2 py-1 text-fg-soft" title={label}>{label}</td>
              <td className="px-2 py-1 text-right font-mono" title={`${percent(row.weight_pct!)} ÷ ${percent(denominator)}`}>{percent(row.weight_pct!)}</td>
              <td className="px-2 py-1 font-mono text-fg-muted whitespace-nowrap">{hasNativePrices ? <>{price(row.return_start_price_local!, row.return_currency!)} <span className="text-fg-faint">({row.return_start_date}) →</span> {price(row.return_end_price_local!, row.return_currency!)} <span className="text-fg-faint">({row.return_end_date})</span></> : '—'}</td>
              <td className="px-2 py-1 font-mono text-fg-muted whitespace-nowrap">{hasEurPrices ? <>€{row.return_start_price_eur!.toFixed(2)} <span className="text-fg-faint">→</span> €{row.return_end_price_eur!.toFixed(2)}</> : '—'}</td>
              <td className="px-2 py-1 text-right font-mono text-fg">{percent(row.return_pct!, true)}</td>
              <td className="px-2 py-1 text-right font-mono text-fg">{points(contribution)}</td>
            </tr>
          );
        })}
          </tbody>
        </table>
      </div>
      <span className="flex items-baseline justify-between gap-3 border-t border-neutral-700/50 mt-1.5 pt-1.5">
        <span className="text-fg-muted">{copy.sum}</span>
        <strong className="font-mono tabular-nums text-fg-strong shrink-0">{result}</strong>
      </span>
    </div>
  );
}
