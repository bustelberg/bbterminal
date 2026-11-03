export type WeeklyCell = {
  status: string; return_pct: number | null; price: number | null; from_price?: number | null;
};
export type WeeklyInstrument = {
  id: string; name: string; isin: string | null; currency: string; held: boolean | null; weeks: WeeklyCell[];
};
export type WeeklyReturns = {
  portefeuille: string; start_date: string; through: string; missing_observations: number;
  periods: { week_end: string; target_date: string; as_of_date: string | null; from_date: string | null; partial: boolean; available: boolean }[];
  options: InstrumentOption[]; selected: WeeklyInstrument | null;
};
export type InstrumentOption = Pick<WeeklyInstrument, 'id' | 'name' | 'isin' | 'currency'>;

/** Missing observations remain gaps, never zero-return bars. */
export function weeklyBars(cells: WeeklyCell[]): (number | null)[] {
  return cells.map((cell) => cell.return_pct != null && Number.isFinite(cell.return_pct) ? cell.return_pct : null);
}

/** A price return uses the opening and closing quotes, never a sum of weekly percentages. */
export function periodReturn(data: WeeklyReturns | undefined, window?: [string, string] | null) {
  if (!data?.selected || !data.periods.length) return null;
  const first = window ? data.periods.findIndex((period) => period.target_date === window[0]) : 0;
  const last = window ? data.periods.findIndex((period) => period.target_date === window[1]) : data.periods.length - 1;
  if (first < 0 || last < first) return null;
  const opening = data.selected.weeks[first]?.from_price;
  const closing = data.selected.weeks[last]?.price;
  const from = data.periods[first].from_date;
  const to = data.periods[last].as_of_date;
  if (!from || !to || opening == null || closing == null || !Number.isFinite(opening) || !Number.isFinite(closing)
      || opening <= 0 || closing <= 0) return null;
  const value = (closing / opening - 1) * 100;
  return Number.isFinite(value) ? { value, from, to } : null;
}

export function instrumentLabel(instrument: InstrumentOption): string {
  return [instrument.name, instrument.currency, instrument.isin].filter(Boolean).join(' · ');
}

export function searchInstruments(options: InstrumentOption[], query: string): InstrumentOption[] {
  const needle = query.trim().toLowerCase();
  return options.filter((option) => instrumentLabel(option).toLowerCase().includes(needle));
}
