import { useLang } from '../../../lib/i18n';
import SimpleExplanation, { useInfoFacts } from './SimpleExplanation';

/** Shared by position rows, asset classes and sold-position totals. */
export default function HoldingsColumnExplanation({ kind, amount, total, value, asOf }: {
  kind: 'weight' | 'contribution'; amount?: number | null; total?: number | null;
  value?: number | null; asOf?: string | null;
}) {
  const [lang] = useLang();
  const nl = lang === 'nl', facts = useInfoFacts();
  const available = value != null && Number.isFinite(value);
  const worked = amount != null && Number.isFinite(amount) && total != null && Number.isFinite(total) && total > 0;
  const money = (number: number) => new Intl.NumberFormat(nl ? 'nl-NL' : 'en-GB', {
    style: 'currency', currency: 'EUR', minimumFractionDigits: 2, maximumFractionDigits: 2,
  }).format(number);
  const result = !available ? '—' : `${kind === 'contribution' && value >= 0 ? '+' : ''}${value.toFixed(2)}${kind === 'weight' ? '%' : 'pp'}`;
  const text = !available
    ? nl ? 'Onvoldoende gegevens om dit cijfer te berekenen.' : 'Not enough data to calculate this figure.'
    : kind === 'weight'
      ? nl ? 'Het deel van de totale portefeuillewaarde dat hierin is belegd, inclusief cash in het totaal.'
        : 'The share of the total portfolio value invested here. The total includes cash.'
      : nl ? 'De bijdrage aan het totale portefeuillerendement, in procentpunten (pp). Een negatieve bijdrage verlaagt het rendement.'
        : 'The contribution to the total portfolio return, in percentage points (pp). A negative contribution reduces the return.';
  return <SimpleExplanation details={<>
    <p>{available && worked ? `${money(amount)} / ${money(total)} = ${result}` : result}</p>
    <p>{facts.dated('AIRS', asOf)}</p>
  </>}>{text}</SimpleExplanation>;
}
