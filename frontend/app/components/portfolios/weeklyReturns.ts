export type WeeklyCell = {
  status: string; return_pct: number | null; price: number | null; from_price?: number | null;
};
export type WeeklyInstrument = {
  id: string; name: string; isin: string | null; currency: string; held: boolean | null; weeks: WeeklyCell[];
};
export type WeeklyReturns = {
  portefeuille: string; through: string; missing_weeks: number;
  periods: { week_end: string; as_of_date: string | null; from_date: string | null; partial: boolean; available: boolean }[];
  instruments: WeeklyInstrument[];
};

/** Break price lines at missing observations and periods when the instrument wasn't held. */
export function priceSegments(cells: WeeklyCell[]): { index: number; price: number }[][] {
  const segments: { index: number; price: number }[][] = [];
  let active: { index: number; price: number }[] | null = null;
  cells.forEach((cell, index) => {
    if (cell.price == null || !Number.isFinite(cell.price)) { active = null; return; }
    if (!active) { active = []; segments.push(active); }
    active.push({ index, price: cell.price });
  });
  return segments;
}
