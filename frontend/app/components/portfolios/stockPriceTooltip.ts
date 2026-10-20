type PriceContext = {
  modelPrice: number | null; modelCurrency?: string | null; modelDate?: string | null;
  nativePrice?: number | null; nativeCurrency?: string | null; nativeDate?: string | null;
};

/** Explain only a real, date-aligned conversion; never infer FX between different quotes. */
export function stockPriceTooltip(price: PriceContext): string {
  const number = (value: number) => value.toLocaleString('en-US', { maximumFractionDigits: 6 });
  const original = price.nativePrice == null ? 'Original Yahoo quote unavailable.'
    : `Original Yahoo close: ${number(price.nativePrice)} ${price.nativeCurrency ?? '(currency unknown)'} (${price.nativeDate ?? 'date unknown'}).`;
  if (price.modelPrice == null || !price.modelCurrency) {
    return `${original}\nNo price in the model currency is available; an unconverted quote is never substituted.`;
  }
  const model = `Valuation price: ${number(price.modelPrice)} ${price.modelCurrency} (${price.modelDate ?? 'date unknown'}).`;
  if (price.nativePrice == null || price.nativePrice <= 0 || !price.nativeCurrency
      || !price.modelDate || price.modelDate !== price.nativeDate) {
    return `${original}\n${model}\nThese quotes are not date-aligned; no FX rate is inferred between them.`;
  }
  if (price.nativeCurrency === price.modelCurrency) return `${original}\n${model}\nNo FX conversion needed.`;
  const factor = price.modelPrice / price.nativePrice;
  return `${original}\nApplied conversion: 1 ${price.nativeCurrency} = ${number(factor)} ${price.modelCurrency}.`
    + `\n${number(price.nativePrice)} × ${number(factor)} ≈ ${number(price.modelPrice)} ${price.modelCurrency}.`
    + '\nUses stored FX on or before the quote date, including quote-unit scaling where applicable. The same converted price is used by every valuation model.';
}
