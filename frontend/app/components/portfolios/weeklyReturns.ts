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

/** Pearson correlation of paired weekly returns over identical actual quote intervals. */
export function weeklyCorrelation(first: WeeklyReturns | null, second: WeeklyReturns | null, window: [string, string] | null = null) {
  const other = new Map<string, number>();
  const key = (period: WeeklyReturns['periods'][number]) => `${period.target_date}|${period.from_date}|${period.as_of_date}`;
  second?.periods.forEach((period, index) => {
    const value = second.selected?.weeks[index]?.return_pct;
    if (period.from_date && period.as_of_date && value != null && Number.isFinite(value)) other.set(key(period), value);
  });
  const pairs: [number, number][] = [];
  first?.periods.forEach((period, index) => {
    if (window && (period.target_date < window[0] || period.target_date > window[1])) return;
    const a = first.selected?.weeks[index]?.return_pct;
    const b = other.get(key(period));
    if (a != null && Number.isFinite(a) && b != null) pairs.push([a, b]);
  });
  const count = pairs.length;
  if (count < 3) return { value: null, count };
  if (pairs.every(([a]) => a === pairs[0][0]) || pairs.every(([, b]) => b === pairs[0][1])) return { value: null, count };
  const scale = Math.max(1, ...pairs.flatMap(([a, b]) => [Math.abs(a), Math.abs(b)]));
  const meanA = pairs.reduce((sum, [a]) => sum + a / scale, 0) / count;
  const meanB = pairs.reduce((sum, [, b]) => sum + b / scale, 0) / count;
  let covariance = 0, varianceA = 0, varianceB = 0;
  pairs.forEach(([a, b]) => {
    const x = a / scale - meanA, y = b / scale - meanB;
    covariance += x * y; varianceA += x * x; varianceB += y * y;
  });
  const value = covariance / Math.sqrt(varianceA * varianceB);
  return { value: Number.isFinite(value) ? Math.max(-1, Math.min(1, value)) : null, count };
}

export function instrumentLabel(instrument: InstrumentOption): string {
  return [instrument.name, instrument.currency, instrument.isin].filter(Boolean).join(' · ');
}

export function searchInstruments(options: InstrumentOption[], query: string): InstrumentOption[] {
  const needle = query.trim().toLowerCase();
  return options.filter((option) => instrumentLabel(option).toLowerCase().includes(needle));
}
