import type { ModelPortfolioAnalysis } from '../../../lib/types/api';

type SoldPosition = NonNullable<NonNullable<ModelPortfolioAnalysis['realised']>['positions']>[number];
export type SoldDisplayRow = SoldPosition & {
  row_key: string;
  via_certificate?: string;
  certificate_weight_pct?: number;
  composition_as_of?: string;
};

/** Display-only expansion. Child rows never inherit the wrapper's accounting amounts. */
export function soldLookThroughRows(positions: SoldPosition[], lookThrough: boolean): SoldDisplayRow[] {
  return positions.flatMap((parent, index) => {
    const key = `${index}:${parent.isin ?? parent.name}`;
    const wrapper: SoldDisplayRow = { ...parent, row_key: key };
    if (!lookThrough) return [wrapper];
    return [wrapper, ...(parent.lookthrough_positions ?? []).map((child, childIndex): SoldDisplayRow => ({
      row_key: `${key}:${childIndex}:${child.isin ?? child.name}`,
      name: child.name,
      isin: child.isin,
      closed_out: true,
      last_sale: parent.last_sale,
      via_certificate: parent.name,
      certificate_weight_pct: child.weight_pct,
      composition_as_of: child.composition_as_of,
      since_close_pct: child.since_close_pct,
      since_close_date: child.since_close_date,
      since_close_as_of: child.since_close_as_of,
      since_close_from: child.since_close_from,
      since_close_to: child.since_close_to,
    }))];
  });
}
