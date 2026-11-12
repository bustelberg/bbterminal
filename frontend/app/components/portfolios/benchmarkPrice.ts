/** FX is quoted as units of the original currency per EUR, as in the API return calculation. */
export function benchmarkPriceEur(price: number | null | undefined, currency: string | null | undefined,
  fx: number | null | undefined): number | null {
  if (price == null || !Number.isFinite(price) || price <= 0 || !currency) return null;
  if (currency.toUpperCase() === 'EUR') return price;
  if (fx == null || !Number.isFinite(fx) || fx <= 0) return null;
  const converted = price / fx;
  return Number.isFinite(converted) ? converted : null;
}
