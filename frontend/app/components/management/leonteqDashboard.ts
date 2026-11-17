import type { Basket } from '../portfolios/types';

export const LEONTEQ_DASHBOARD = '/api/leonteq/management/topselecties';
export type LeonteqDetail = {
  success: boolean;
  composition?: { groups?: { positions: { name: string; isin?: string | null; weight: string }[] }[] };
};

export function leonteqBasket(name: string, source: LeonteqDetail): Basket | null {
  if (!source.success) return null;
  const positions = source.composition?.groups?.flatMap(group => group.positions) ?? [];
  const holdings: Basket['holdings'] = [];
  for (const position of positions) {
    const text = position.weight.replace(/[’'\s]/g, '').replace(/%$/, '');
    if (!/^[+-]?\d+(?:\.\d+)?$/.test(text)) return null;
    const weight = Number(text);
    if (!Number.isFinite(weight) || weight < 0) return null;
    if (position.isin && weight > 0) holdings.push({ isin: position.isin, weight, name: position.name });
  }
  return holdings.length ? { label: `${name} · Leonteq`, holdings } : null;
}
