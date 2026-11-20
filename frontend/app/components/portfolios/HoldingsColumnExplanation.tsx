import { useLang } from '../../../lib/i18n';
import { createContext, useContext } from 'react';
import SimpleExplanation, { useInfoFacts } from './SimpleExplanation';

export const HoldingsColumnSource = createContext('AIRS');
export const HoldingsAccountingContext = createContext<{ available: boolean; note?: string | null } | null>(null);

export function useHoldingReturnLabel() {
  const [lang] = useLang();
  const source = useContext(HoldingsColumnSource);
  return lang === 'nl' ? `${source}-rendement` : `${source} return`;
}

/** Shared by position rows, asset classes and sold-position totals. */
export default function HoldingsColumnExplanation({ kind, amount, total, value, asOf, toppenberg }: {
  kind: 'weight' | 'contribution'; amount?: number | null; total?: number | null;
  value?: number | null; asOf?: string | null; toppenberg?: boolean;
}) {
  const [lang] = useLang();
  const source = useContext(HoldingsColumnSource);
  const accounting = useContext(HoldingsAccountingContext);
  const nl = lang === 'nl', facts = useInfoFacts();
  const available = value != null && Number.isFinite(value);
  const worked = amount != null && Number.isFinite(amount) && total != null && Number.isFinite(total) && total > 0;
  const money = (number: number) => new Intl.NumberFormat(nl ? 'nl-NL' : 'en-GB', {
    style: 'currency', currency: 'EUR', minimumFractionDigits: 2, maximumFractionDigits: 2,
  }).format(number);
  const result = !available ? '—' : `${kind === 'contribution' && value >= 0 ? '+' : ''}${value.toFixed(2)}${kind === 'weight' ? '%' : 'pp'}`;
  if (toppenberg) {
    const explanation = kind === 'weight'
      ? (nl ? 'Het TopSelectie-gewicht komt uit AIRS. Bij doorkijk telt ook het Leonteq-gewicht van de onderliggende positie mee.'
        : 'The TopSelectie weight comes from AIRS. Expanded positions also use their weight within the Leonteq sleeve.')
      : (nl ? 'Genormaliseerd AIRS-gewicht maal het Leonteq-rendement over de gezamenlijke periode. Voor cash rekenen we met 0%.'
        : 'Normalized AIRS weight times the Leonteq return over the shared period. Cash assumes a 0% return.');
    return <SimpleExplanation details={<>
      <p>{result}</p>
      <p>{facts.weights(asOf)}</p>
    </>}>{explanation}</SimpleExplanation>;
  }
  const text = !available
    ? source === 'Leonteq' && kind === 'contribution'
      ? accounting?.note ?? (nl ? 'Leonteq-bijdrage niet beschikbaar: openingswaarderingen of netto transacties ontbreken.'
        : 'Leonteq contribution unavailable: opening valuations or net transactions are incomplete.')
      : nl ? 'Onvoldoende gegevens om dit cijfer te berekenen.' : 'Not enough data to calculate this figure.'
    : kind === 'weight'
      ? nl ? 'Het deel van de totale portefeuillewaarde dat hierin is belegd, inclusief cash in het totaal.'
        : 'The share of the total portfolio value invested here. The total includes cash.'
      : source === 'Leonteq'
        ? nl ? 'Berekend uit netto Leonteq-transacties en gedateerde waarderingen. Dagelijkse bijdragen worden gekoppeld tot de bijdrage aan het portefeuillerendement, in procentpunten.'
          : 'Calculated from Leonteq net transactions and dated valuations. Daily contributions are geometrically linked into portfolio return contributions, in percentage points.'
      : nl ? 'De bijdrage aan het totale portefeuillerendement, in procentpunten (pp). Een negatieve bijdrage verlaagt het rendement.'
        : 'The contribution to the total portfolio return, in percentage points (pp). A negative contribution reduces the return.';
  return <SimpleExplanation details={<>
    <p>{available && worked && !(source === 'Leonteq' && kind === 'contribution') ? `${money(amount)} / ${money(total)} = ${result}` : result}</p>
    {source === 'Leonteq' && kind === 'contribution' && available && accounting?.note && <p>{accounting.note}</p>}
    <p>{facts.dated(source, asOf)}</p>
  </>}>{text}</SimpleExplanation>;
}
