import { valuationInputs, workbookValuation, VALUATION_FORMULAS, type CalculationKey, type Workbook, type WorkbookRow } from './valuationWorkbook';
import { COLUMN_STYLE, WORKBOOK_COLUMNS } from './valuationWorkbookColumns';

const explanations: Record<CalculationKey, [string, string]> = {
  valuationCurrency: ['Use the Yahoo quote currency; convert pence units to pounds.', 'Gebruik de Yahoo-koersvaluta; zet pence om naar ponden.'],
  unitScale: ['Use 0.01 for prices quoted in pence, otherwise 1.', 'Gebruik 0,01 voor koersen in pence, anders 1.'],
  price: ['Convert the raw Yahoo close to the valuation currency. Quotes older than seven days need refreshing.', 'Zet de ruwe Yahoo-slotkoers om naar de waarderingsvaluta. Koersen ouder dan zeven dagen moeten worden vernieuwd.'],
  calendarMonths: ['Count calendar months from the price date through the first forecast year-end, limited to 0–12.', 'Tel kalendermaanden vanaf de koersdatum tot het eerste voorspelde jaareinde, begrensd op 0–12.'],
  months: ['Use your month override when entered; otherwise use the calendar-month count.', 'Gebruik de ingevoerde maanden, of anders het aantal kalendermaanden.'],
  basis: ['Determine the per-share earnings, NAV or book value in the quote currency.', 'Bepaal de winst, NAV of boekwaarde per aandeel in de koersvaluta.'],
  currentMultiple: ['Divide the current stock price by the selected per-share valuation basis.', 'Deel de actuele koers door de gekozen waarderingsbasis per aandeel.'],
  fair: ['Multiply the selected per-share valuation basis by your fair multiple.', 'Vermenigvuldig de gekozen waarderingsbasis per aandeel met uw faire multiple.'],
  fairUpside: ['Compare the fair valuation with the current stock price, expressed as a percentage.', 'Vergelijk de faire waardering met de actuele koers, uitgedrukt als percentage.'],
  buyMultiple: ['Reduce your fair multiple by the safety margin: fair multiple × (1 − margin).', 'Verlaag uw faire multiple met de veiligheidsmarge: faire multiple × (1 − marge).'],
  buyPrice: ['Apply your fair multiple and then deduct the safety margin from the valuation.', 'Pas uw faire multiple toe en trek vervolgens de veiligheidsmarge af van de waardering.'],
  buyUpside: ['Compare the buy valuation with the current stock price, expressed as a percentage.', 'Vergelijk de koopwaardering met de actuele koers, uitgedrukt als percentage.'],
  signal: ['Buy at or below the buy valuation; consider up to fair valuation. Missing inputs or a review flag require review.', 'Koop op of onder de koopwaardering; overweeg tot de faire waardering. Ontbrekende invoer of een toetsingsvlag vereist controle.'],
  bear: ['Grow the selected per-share basis over the projection period using the bear growth adjustment (minimum −90%); apply the adjusted exit multiple.', 'Laat de gekozen basis groeien over de projectieperiode met de negatieve groeiaanpassing (minimaal −90%); pas de aangepaste exitmultiple toe.'],
  base: ['Grow the selected per-share basis over the projection period, then apply your exit multiple.', 'Laat de gekozen basis groeien over de projectieperiode en pas uw exitmultiple toe.'],
  bull: ['Grow the selected per-share basis using the bull growth adjustment, then apply the adjusted exit multiple.', 'Laat de gekozen basis groeien met de positieve groeiaanpassing en pas de aangepaste exitmultiple toe.'],
  cagr: ['Express the change from today’s price to the base scenario valuation as a compound annual return.', 'Druk de verandering van de huidige koers naar de basiswaardering uit als samengesteld jaarrendement.'],
};

const format = (value: unknown) => value == null || (typeof value === 'number' && !Number.isFinite(value)) ? '—'
  : typeof value === 'number' ? value.toLocaleString(undefined, { maximumFractionDigits: 4 }) : String(value);

export default function ValuationFormulaExplanation({ calculation, workbook, row, nl, onInput }: {
  calculation: CalculationKey; workbook: Workbook; row?: WorkbookRow; nl: boolean; onInput: (key: string) => void;
}) {
  const t = (en: string, dutch: string) => nl ? dutch : en;
  const definition = VALUATION_FORMULAS[calculation];
  const inputs = row ? valuationInputs(row, row.quote ?? {}, workbook.parameters, workbook.as_of) : null;
  const result = row ? workbookValuation(row, row.quote ?? {}, workbook.parameters, workbook.as_of) : null;
  const manual = row?.assumptions.basis_kind === 'manual';
  const hasBasis = definition.inputs.some(key => key === 'basis_kind');
  const inactive = manual ? ['fy1', 'fy2', 'fy1_date', 'months_fy1', 'forecast_currency'] : ['manual_basis', 'manual_currency'];
  const usesPrice = definition.inputs.some(key => key === 'raw_price');
  if (manual && !usesPrice) inactive.push('price_date');
  if (!manual && row?.assumptions.months_fy1 != null) {
    inactive.push('fy1_date');
    if (!usesPrice) inactive.push('price_date');
  }
  const normalize = (code?: string | null) => code === 'GBp' || code === 'GBX' ? 'GBP' : code;
  const sourceCurrency = manual ? row?.assumptions.basis_currency : row?.forecast.currency;
  if (sourceCurrency && normalize(sourceCurrency) === normalize(row?.quote?.currency)) inactive.push('basis_fx');
  const percent = ['growth', 'safety_margin', 'bear_growth_delta', 'bull_growth_delta', 'bear_multiple_delta', 'bull_multiple_delta'];
  const resultUnit = ['fairUpside', 'buyUpside', 'cagr'].includes(calculation) ? '%'
    : ['currentMultiple', 'buyMultiple'].includes(calculation) ? '×'
    : ['price', 'basis', 'fair', 'buyPrice', 'bear', 'base', 'bull'].includes(calculation) ? result?.valuationCurrency ?? '' : '';
  return <div className="space-y-4 text-sm">
    <p className="font-medium">{explanations[calculation][nl ? 1 : 0]}</p>
    {hasBasis && <ol className="list-decimal space-y-2 pl-5">
      <li>{manual
        ? t('Use the entered source value per share (earnings, NAV or book value). Forecast EPS and month weights are not used.', 'Gebruik de ingevoerde bronwaarde per aandeel (winst, NAV of boekwaarde). Prognosewinst en maandweging tellen niet mee.')
        : t('Blend the first two forecast years: FY1 EPS × M/12 + FY2 EPS × (12−M)/12. M is your month override, or the calendar months from the price date through FY1 year-end (0–12).', 'Weeg de eerste twee prognosejaren: FY1 EPS × M/12 + FY2 EPS × (12−M)/12. M is uw maandinvoer, of de kalendermaanden vanaf de koersdatum tot het einde van FY1 (0–12).')}</li>
      <li>{t('Convert pence to pounds where needed. If the normalized source and quote currencies differ, apply your FX input; otherwise use 1.', 'Zet pence waar nodig om naar ponden. Verschillen de genormaliseerde bron- en koersvaluta, pas dan uw FX-invoer toe; gebruik anders 1.')}</li>
      <li>{explanations[calculation][nl ? 1 : 0]}</li>
    </ol>}
    <div className={`rounded-lg border p-3 ${COLUMN_STYLE.calculation}`}>
      <span className="mr-3">{t('Result for this company', 'Uitkomst voor dit bedrijf')}</span>
      <strong className="text-lg tabular-nums">{format(result?.[calculation])}{result?.[calculation] != null && ` ${resultUnit}`}</strong>
      {result?.[calculation] == null && <p className="mt-1 text-xs">{result?.reason ?? t('Required inputs are missing or invalid.', 'Benodigde invoer ontbreekt of is ongeldig.')}</p>}
    </div>
    <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3" aria-label={t('Formula inputs', 'Formule-invoer')}>
      {[...new Set(definition.inputs)].filter(key => !hasBasis || !inactive.includes(key)).map(key => {
        const column = WORKBOOK_COLUMNS.find(item => item.key === key);
        const kind = column?.kind ?? (key === 'as_of' ? 'data' : 'assumption');
        return <button key={key} data-input={key} type="button" onClick={() => onInput(key)}
          className={`cursor-pointer rounded-lg border p-3 text-left ${COLUMN_STYLE[kind]}`}>
          <span className="block text-xs">{column ? nl ? column.nl : column.en : key}</span>
          <strong className="mt-1 block tabular-nums">{typeof inputs?.[key] === 'number' && percent.includes(key)
            ? `${format(Number(inputs[key]) * 100)}%` : format(inputs?.[key])}{inputs?.[key] != null && ['fair_multiple', 'exit_multiple'].includes(key) ? '×' : ''}</strong>
        </button>;
      })}
    </div>
    <details className="rounded-lg border border-neutral-800/30 p-3">
      <summary className="cursor-pointer">{t('Full formula · blue and green inputs only', 'Volledige formule · alleen blauwe en groene invoer')}</summary>
      <p className="mt-3 whitespace-normal break-words font-mono text-xs leading-relaxed">{definition.expression}</p>
    </details>
  </div>;
}
