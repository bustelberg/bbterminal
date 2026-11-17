import Formula from '../../../lib/formula';
import { valuationInputs, VALUATION_FORMULAS, type CalculationKey, type Workbook, type WorkbookRow } from './valuationWorkbook';
import { WORKBOOK_COLUMNS } from './valuationWorkbookColumns';
import { valuationFormulaLatex } from './valuationFormulaLatex';

export default function ValuationFormulaExplanation({ calculation, workbook, row, nl, onInput }: {
  calculation: CalculationKey; workbook: Workbook; row?: WorkbookRow; nl: boolean; onInput: (key: string) => void;
}) {
  const t = (en: string, dutch: string) => nl ? dutch : en;
  const definition = VALUATION_FORMULAS[calculation];
  const latex = valuationFormulaLatex(calculation, workbook, row);
  const inputs = row ? valuationInputs(row, row.quote ?? {}, workbook.parameters, workbook.as_of) : null;
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
  return <div className="space-y-4 text-sm">
    <section aria-label={t('Formula', 'Formule')} className="rounded-lg border border-neutral-800/40 p-3">
      <Formula tex={latex.symbolic} />
      <dl aria-label={t('Variables', 'Variabelen')} className="mt-3 grid grid-cols-[auto_1fr] items-baseline gap-x-4 gap-y-2 text-xs text-fg-muted">
        {latex.variables.map(variable => <div key={variable.symbol} className="contents">
          <dt><Formula tex={variable.symbol} inline /></dt>
          <dd>{nl ? variable.nl : variable.en}</dd>
        </div>)}
      </dl>
    </section>
    <section aria-label={t('With company data', 'Met bedrijfsgegevens')} className="rounded-lg border border-neutral-800/40 p-3">
      {latex.substituted ? <Formula tex={latex.substituted} /> : <p className="text-center text-fg-muted">{latex.reason}</p>}
    </section>
    <details className="text-xs text-fg-muted">
      <summary className="cursor-pointer">{t('Edit inputs', 'Invoer wijzigen')}</summary>
      <div className="mt-2 flex flex-wrap gap-2" aria-label={t('Formula inputs', 'Formule-invoer')}>
        {[...new Set(definition.inputs)].filter(key => !hasBasis || !inactive.includes(key)).map(key => {
          const column = WORKBOOK_COLUMNS.find(item => item.key === key);
          return <button key={key} data-input={key} type="button" onClick={() => onInput(key)}
            title={inputs?.[key] == null ? undefined : String(inputs[key])}
            className="rounded-lg border border-neutral-800/40 px-3 py-2">
            {column ? nl ? column.nl : column.en : key}
          </button>;
        })}
      </div>
    </details>
  </div>;
}
