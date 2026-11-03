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

export function instrumentLabel(instrument: InstrumentOption): string {
  return [instrument.name, instrument.currency, instrument.isin].filter(Boolean).join(' · ');
}

export function searchInstruments(options: InstrumentOption[], query: string): InstrumentOption[] {
  const needle = query.trim().toLowerCase();
  return options.filter((option) => instrumentLabel(option).toLowerCase().includes(needle));
}
