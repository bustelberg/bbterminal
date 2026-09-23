'use client';

import { useLang } from '../../../lib/i18n';

export type ReturnCalculationRow = {
  name?: string | null;
  ticker?: string | null;
  isin?: string | null;
  weight_pct?: number | null;
  return_pct?: number | null;
};

const percent = (value: number, signed = false) =>
  `${signed && value >= 0 ? '+' : ''}${value.toFixed(2)}%`;
const points = (value: number) => `${value >= 0 ? '+' : ''}${value.toFixed(2)}pp`;

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
  } : {
    heading: 'Actual values in the calculation',
    intro: `${owner} in ${bucket}: each holding contributes in proportion to its opening weight.`,
    sum: `Sum of ${calculation.rows.length} contributions to the return`,
  };

  return (
    <span className="block rounded-md border border-neutral-800/40 bg-overlay/[0.05] px-2.5 py-2">
      <span className="block text-[10px] uppercase tracking-wider text-fg-faint mb-1">
        {copy.heading}
      </span>
      <span className="block text-[11px] leading-relaxed text-fg-muted mb-1.5">
        {copy.intro}
      </span>
      <span className="block max-h-56 overflow-y-auto pr-1 font-mono tabular-nums">
        {calculation.rows.map(({ row, contribution }, index) => {
          const label = row.name ?? row.ticker ?? row.isin ?? `#${index + 1}`;
          return (
            <span key={`${row.isin ?? row.ticker ?? label}-${index}`}
              className="block border-t border-neutral-800/15 py-1 first:border-t-0">
              <span className="block truncate text-fg-muted" title={label}>{label}</span>
              <span className="block text-fg">
                ({percent(row.weight_pct!)} &divide; {percent(denominator)}) &times;{' '}
                {percent(row.return_pct!, true)} = {points(contribution)}
              </span>
            </span>
          );
        })}
      </span>
      <span className="flex items-baseline justify-between gap-3 border-t border-neutral-700/50 mt-1.5 pt-1.5">
        <span className="text-fg-muted">{copy.sum}</span>
        <strong className="font-mono tabular-nums text-fg-strong shrink-0">{result}</strong>
      </span>
    </span>
  );
}
