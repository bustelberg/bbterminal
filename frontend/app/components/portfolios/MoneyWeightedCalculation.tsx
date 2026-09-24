'use client';

export type MoneyWeightedCashFlow = {
  date?: string | null;
  amount_eur?: number | null;
  kind?: string | null;
  source?: string | null;
};
type ValidCashFlow = MoneyWeightedCashFlow & { date: string; amount_eur: number };

const day = (iso: string) => Date.parse(`${iso.slice(0, 10)}T00:00:00Z`);
const daysFrom = (iso: string, start: string) => Math.round((day(iso) - day(start)) / 86_400_000);
const texNumber = (value: number) => Math.abs(value).toLocaleString('en-US', {
  minimumFractionDigits: 2, maximumFractionDigits: 2,
}).replace(/,/g, '{,}');

const validFlows = (flows: MoneyWeightedCashFlow[]) => flows
  .filter((f): f is ValidCashFlow =>
    Boolean(f.date) && f.amount_eur != null && f.amount_eur !== 0)
  .sort((a, b) => a.date.localeCompare(b.date));

/** The exact dated equation solved by the backend, followed by its holding-period conversion. */
export function moneyWeightedWorked(flows: MoneyWeightedCashFlow[] | null | undefined,
  cumulativePct: number | null | undefined): string | undefined {
  const rows = validFlows(flows ?? []);
  if (rows.length < 2 || cumulativePct == null) return undefined;
  const start = rows[0].date;
  const span = daysFrom(rows.at(-1)!.date, start);
  if (span <= 0 || cumulativePct <= -100) return undefined;
  const term = (f: ValidCashFlow) => `\\frac{\\mathrm{EUR}\\,${texNumber(f.amount_eur)}}`
    + `{(1+r)^{${daysFrom(f.date, start)}/365}}`;
  // Present the same XIRR equation in the reader's natural balance-sheet form: discounted money
  // invested equals discounted money received. Moving the negative operands to the left is
  // algebraically identical to the solver's signed-cash-flow sum equalling zero.
  const invested = rows.filter((f) => f.amount_eur < 0).map(term).join('+');
  const received = rows.filter((f) => f.amount_eur > 0).map(term).join('+');
  const annual = (Math.pow(1 + cumulativePct / 100, 365 / span) - 1) * 100;
  const pct = `${cumulativePct >= 0 ? '+' : ''}${cumulativePct.toFixed(2)}\\%`;
  const annualPct = `${annual >= 0 ? '+' : ''}${annual.toFixed(4)}\\%`;
  return `\\begin{aligned}${invested}&=${received}\\\\[6pt]`
    + `r_{\\mathrm{annual}}&\\approx ${annualPct}\\\\[6pt]`
    + `R_{\\mathrm{period}}&=(1+r_{\\mathrm{annual}})^{${span}/365}-1=${pct}`
    + `\\end{aligned}`;
}

const KIND: Record<string, { en: string; nl: string }> = {
  'opening value': { en: 'Opening value', nl: 'Openingswaarde' },
  purchase: { en: 'Purchase', nl: 'Aankoop' },
  sale: { en: 'Sale', nl: 'Verkoop' },
  dividend: { en: 'Dividend', nl: 'Dividend' },
  'dividend withholding tax': { en: 'Dividend withholding tax', nl: 'Dividendbelasting' },
  'net income': { en: 'Net income', nl: 'Netto-inkomsten' },
  'net income (date unavailable)': {
    en: 'Net income (booked at period end)', nl: 'Netto-inkomsten (op einddatum geboekt)',
  },
  'final valuation': { en: 'Final valuation', nl: 'Eindwaardering' },
};

const SHORT_MONTHS = {
  en: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
  nl: ['jan', 'feb', 'mrt', 'apr', 'mei', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dec'],
} as const;

/** Compact, unambiguous display date for the AIRS-input table (for example `1 jan 2026`). */
export function shortAirsDate(iso: string, lang: 'en' | 'nl'): string {
  const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso);
  if (!match) return iso;
  const month = Number(match[2]) - 1;
  const date = Number(match[3]);
  if (month < 0 || month > 11 || date < 1 || date > 31) return iso;
  return `${date} ${SHORT_MONTHS[lang][month]} ${match[1]}`;
}

/** Every raw/derived AIRS operand passed to XIRR, in solver order. */
export function MoneyWeightedCashflowCalculation({ flows, lang }: {
  flows: MoneyWeightedCashFlow[] | null | undefined;
  lang: 'en' | 'nl';
}) {
  const rows = validFlows(flows ?? []);
  if (!rows.length) return null;
  const start = rows[0].date;
  const money = new Intl.NumberFormat(lang === 'nl' ? 'nl-NL' : 'en-US', {
    style: 'currency', currency: 'EUR', minimumFractionDigits: 2, maximumFractionDigits: 2,
  });
  return (
    <span className="mt-2 block rounded-md border border-neutral-800/40 bg-overlay/[0.05] p-2.5">
      <span className="mb-1.5 block text-[10px] font-semibold uppercase tracking-wide text-fg-faint">
        {lang === 'nl' ? 'Werkelijke invoer uit AIRS' : 'Actual AIRS inputs'}
      </span>
      <span className="block overflow-x-auto">
        <table className="w-full min-w-[26rem] text-[11px]">
          <thead className="text-fg-faint">
            <tr>
              <th className="pb-1 pr-3 text-left font-medium">{lang === 'nl' ? 'Datum' : 'Date'}</th>
              <th className="pb-1 pr-3 text-left font-medium">{lang === 'nl' ? 'Invoer' : 'Input'}</th>
              <th className="pb-1 pr-3 text-left font-medium">{lang === 'nl' ? 'Bron' : 'Source'}</th>
              <th className="pb-1 pr-3 text-right font-medium">{lang === 'nl' ? 'Dagen' : 'Days'}</th>
              <th className="pb-1 text-right font-medium">{lang === 'nl' ? 'Kasstroom' : 'Cash flow'}</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/30 text-fg-muted">
            {rows.map((f, i) => (
              <tr key={`${f.date}-${f.kind}-${i}`}>
                <td className="whitespace-nowrap py-1 pr-3 font-mono tabular-nums">
                  {shortAirsDate(f.date, lang)}
                </td>
                <td className="py-1 pr-3">{KIND[f.kind ?? '']?.[lang] ?? f.kind ?? '—'}</td>
                <td className="py-1 pr-3">{f.source ?? '—'}</td>
                <td className="py-1 pr-3 text-right font-mono tabular-nums">{daysFrom(f.date, start)}</td>
                <td className={`py-1 text-right font-mono tabular-nums ${f.amount_eur < 0 ? 'text-neg-400' : 'text-pos-400'}`}>
                  {f.amount_eur >= 0 ? '+' : '−'}{money.format(Math.abs(f.amount_eur))}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </span>
      <span className="mt-1.5 block text-[10.5px] leading-snug text-fg-faint">
        {lang === 'nl'
          ? 'Negatief = geld ingelegd; positief = geld ontvangen. Openingswaarde wordt uit AIRS VOLK en de AIRS-transacties herleid.'
          : 'Negative = money invested; positive = money received. Opening value is reconstructed from AIRS VOLK and AIRS transactions.'}
      </span>
    </span>
  );
}
