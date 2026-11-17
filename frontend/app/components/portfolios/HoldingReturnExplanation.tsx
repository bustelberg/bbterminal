import { useLang } from '../../../lib/i18n';
import type { ModelPortfolioAnalysis } from '../../../lib/types/api';
import SimpleExplanation, { useInfoFacts } from './SimpleExplanation';
import { dayOf } from './asOfLine';
import { useContext } from 'react';
import { HoldingsColumnSource } from './HoldingsColumnExplanation';

type Holding = NonNullable<ModelPortfolioAnalysis['book_holdings']>[number];
const pct = (value?: number | null) => value == null ? '—' : `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`;

export default function HoldingReturnExplanation({ holding: h, asOf, parentBook }: { holding: Holding; asOf?: string | null; parentBook?: string | null }) {
  const [lang] = useLang();
  const source = useContext(HoldingsColumnSource);
  const nl = lang === 'nl', facts = useInfoFacts();
  const legs = (h.sources ?? []).filter((leg) => leg.blend_weight_pct != null && leg.return_pct != null);
  const yahoo = h.own_return_source === 'yfinance';
  const legDates = legs.map((leg) => dayOf(leg.as_of)).filter((date): date is string => date != null).sort();
  const canUseParentDate = !yahoo && legs.length <= 1 && (!h.own_return_book || h.own_return_book === parentBook);
  const end = h.own_return_as_of ?? legDates[0] ?? (canUseParentDate ? asOf : null);
  const explanation = h.own_return_pct == null
    ? source === 'Leonteq'
      ? nl ? 'Deze Leonteq-momentopname bevat geen positierendement voor de meetperiode. AVG Performance wordt niet als YTD-rendement gebruikt.'
        : 'This Leonteq snapshot has no position return for the reporting period. AVG Performance is not substituted for a YTD return.'
      : nl ? 'Onvoldoende waarderingsgegevens om dit rendement te berekenen.' : 'Not enough valuation data to calculate this return.'
    : source === 'Leonteq'
      ? h.own_return_source === 'leonteq_average'
        ? nl ? 'AVG Performance zoals gerapporteerd door Leonteq. Dit kan een andere meetperiode hebben dan het YTD-portefeuillerendement en wordt niet gebruikt om bijdrage te berekenen.'
          : 'AVG Performance reported by Leonteq. This may cover a different period from the portfolio YTD return and is not used to calculate contribution.'
      : h.own_return_source === 'leonteq_transactions'
        ? nl ? 'Berekend uit Leonteq: netto resultaat in de meetperiode gedeeld door openingswaarde plus netto aankoopbedragen. Inclusief netto dividend en verkoopresultaat; geen geannualiseerd of tijdgewogen positierendement.'
          : 'Calculated from Leonteq: net period profit divided by opening value plus net purchase outlays. Includes net dividends and sales; not annualised or a time-weighted position return.'
        : nl ? 'Positierendement voor de meetperiode, gerapporteerd door Leonteq.' : 'Position return for the reporting period, reported by Leonteq.'
    : yahoo
      ? nl ? 'Berekend uit opgeslagen koersen, omgerekend naar euro’s.' : 'Calculated from stored prices converted to euros.'
      : legs.length > 1
        ? nl ? 'Combineert de AIRS-rendementen van de portefeuilles die deze positie aanhouden, gewogen naar beginwaarde.'
          : 'Combines AIRS returns from the portfolios holding this position, weighted by starting value.'
        : nl ? 'Gebaseerd op AIRS-waarderingen en toerekenbaar netto dividend. Als dividend niet kan worden gekoppeld, gebruiken we het door AIRS gerapporteerde rendement.'
          : 'Based on AIRS valuations and attributable net dividends. If dividends cannot be matched, we use the return reported by AIRS.';
  return <SimpleExplanation details={<>
    <p>{h.name} · {pct(h.own_return_pct)}{h.own_return_source === 'leonteq_average' ? ' · Leonteq AVG Performance' : ' · EUR'}</p>
    <p>{yahoo ? 'Yahoo Finance' : source}{h.own_return_book ? ` · ${h.own_return_book}` : ''} · {facts.window(h.own_return_from, end)}</p>
    {legs.length > 1 && legs.map((leg, index) => <p key={index}>{leg.label ?? leg.book ?? 'AIRS'}: {leg.blend_weight_pct?.toFixed(2)}% × {pct(leg.return_pct)} · {facts.date(leg.as_of)}</p>)}
    {h.own_return_estimated && <p>{nl ? 'De beginkoers is geschat.' : 'The opening price is estimated.'}</p>}
  </>}>{explanation}</SimpleExplanation>;
}
