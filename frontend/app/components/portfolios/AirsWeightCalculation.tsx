'use client';

import { useLang } from '../../../lib/i18n';

export type AirsWeightComponent = {
  name: string;
  value_eur: number;
};

const money = (value: number, locale: string) => new Intl.NumberFormat(locale, {
  style: 'currency', currency: 'EUR', minimumFractionDigits: 2, maximumFractionDigits: 2,
}).format(value);

/** The literal AIRS rows behind one opening weight—no symbolic algebra or hidden subtotal. */
export default function AirsWeightCalculation({ components, numerator, denominator, result,
  holdingName }: {
  components: AirsWeightComponent[];
  numerator: number;
  denominator: number;
  result: string;
  holdingName: string;
}) {
  const [lang] = useLang();
  const locale = lang === 'nl' ? 'nl-NL' : 'en-GB';
  const copy = lang === 'nl' ? {
    heading: 'Werkelijke AIRS-waarden (VOLK)',
    sum: `Som van ${components.length} Beginwaarde-regels`,
    result: 'Uiteindelijke weging',
  } : {
    heading: 'Actual AIRS values (VOLK)',
    sum: `Sum of ${components.length} Beginwaarde rows`,
    result: 'Resulting weight',
  };

  return (
    <span className="block rounded-md border border-neutral-800/40 bg-overlay/[0.05] px-2.5 py-2">
      <span className="block text-[10px] uppercase tracking-wider text-fg-faint mb-1.5">
        {copy.heading}
      </span>
      <span className="block max-h-52 overflow-y-auto pr-1 font-mono tabular-nums">
        {components.map((component, index) => (
          <span key={`${component.name}-${index}`} className="flex items-baseline gap-1 py-0.5">
            <span className="w-3 shrink-0 text-fg-faint">{index === 0 ? '' : '+'}</span>
            <span className="min-w-0 flex-1 truncate text-fg-muted" title={component.name}>
              {component.name}
            </span>
            <span className="shrink-0 text-fg">{money(component.value_eur, locale)}</span>
          </span>
        ))}
      </span>
      <span className="flex items-baseline justify-between gap-2 border-t border-neutral-700/50 mt-1.5 pt-1.5">
        <span className="text-fg-muted">{copy.sum}</span>
        <strong className="font-mono tabular-nums text-fg-strong shrink-0">
          {money(denominator, locale)}
        </strong>
      </span>
      <span className="block border-t border-neutral-700/50 mt-1.5 pt-1.5 text-fg-soft">
        <span className="block text-[10px] uppercase tracking-wider text-fg-faint">
          {copy.result}
        </span>
        <span className="block font-mono tabular-nums break-words">
          {holdingName}: {money(numerator, locale)} ÷ {money(denominator, locale)} = {result}
        </span>
      </span>
    </span>
  );
}
