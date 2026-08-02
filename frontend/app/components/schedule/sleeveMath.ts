/**
 * Sleeve arithmetic for a scheduled strategy's book: cash + ETFs are set by
 * hand, the stock picks take what's left.
 *
 * Everything here is in ABSOLUTE percent — a share of the whole portfolio,
 * which is what a person means by "20% in this ETF". (The backend stores the
 * ETF weight relative to the INVESTED book and converts on the way in; that
 * conversion is deliberately not mirrored here, so the numbers on screen are
 * the numbers held.)
 *
 * Pure + unit-tested: the one thing this must never do is let a book be saved
 * that doesn't add up, and "the stocks get the remainder" is only true while
 * the remainder is non-negative.
 */

export type SleeveEtfDraft = {
  /** null while the row is still being picked. */
  benchmarkId: number | null;
  /** Free text — the input is a string until it's saved. */
  weightPct: string;
};

/** Parse a percent input. Blank / garbage → 0, so a half-typed row never makes
 * the total NaN and disables Save for a reason nobody can see. */
export function parsePct(raw: string): number {
  const n = Number(String(raw).trim());
  return Number.isFinite(n) ? n : 0;
}

/** What the stock sleeve is left with, in percent. Can go negative — that's the
 * over-allocation the caller must refuse. */
export function stockSleevePct(cashPct: number, etfs: SleeveEtfDraft[]): number {
  const etfTotal = etfs.reduce((sum, e) => sum + parsePct(e.weightPct), 0);
  return 100 - cashPct - etfTotal;
}

/**
 * The message to show, or null when the draft is saveable. Returns the FIRST
 * problem only — a form that lists four complaints at once reads as broken.
 */
export function validateSleeves(cashPct: number, etfs: SleeveEtfDraft[]): string | null {
  if (!Number.isFinite(cashPct) || cashPct < 0) return 'Cash can’t be negative.';
  if (cashPct > 100) return 'Cash can’t exceed 100%.';
  const seen = new Set<number>();
  for (const e of etfs) {
    if (e.benchmarkId == null) return 'Pick an ETF for every row (or remove the empty one).';
    if (parsePct(e.weightPct) < 0) return 'An ETF weight can’t be negative.';
    if (seen.has(e.benchmarkId)) return 'The same ETF is listed twice — combine the rows.';
    seen.add(e.benchmarkId);
  }
  const rest = stockSleevePct(cashPct, etfs);
  // Tolerance, not rounding: 10 + 20 + 70 typed by a human must pass, but a
  // genuine over-allocation is refused rather than quietly scaled to fit.
  if (rest < -1e-6) {
    return `Cash + ETFs come to ${(100 - rest).toFixed(2)}% — over 100%. The stocks take what's left.`;
  }
  if (etfs.length > 0 && cashPct >= 100 - 1e-9) {
    return '100% cash leaves nothing to hold an ETF with.';
  }
  return null;
}

/** Round a percent for display without pretending to precision we don't have.
 * `20` → "20", `22.222` → "22.22". */
export function fmtSleevePct(pct: number): string {
  const r = Math.round(pct * 100) / 100;
  return Number.isInteger(r) ? String(r) : r.toFixed(2);
}
