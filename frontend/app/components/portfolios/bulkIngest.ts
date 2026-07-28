/**
 * The work-list + per-holding verdicts behind "fetch financials for every holding" on an empty
 * Long-Equity card. Pure, so the decisions that matter are unit-tested rather than discovered on a
 * 2-minute run against GuruFocus.
 *
 * ⚠ THE WORK-LIST IS THE COVERAGE TABLE, NOT THE METRIC MATRIX. `portfolio-revenue-matrix` skips
 * every holding with no `company` row (`if not c: continue`) — and a `no_company` holding is
 * exactly the one that most needs an ingest. Driving the queue off the matrix would silently
 * leave out the rows the button exists for. The matrix is used only to answer "does this holding
 * ALREADY have this metric", which is the one question coverage cannot answer (its sentinel is
 * Free Cash Flow, not the card's metric).
 *
 * ⚠ `ingested` IS NOT "THIS CARD NOW HAS DATA". A company that pays no dividend ingests perfectly
 * and still has no dividend/share line; a fetch reported as a success beside a chart that stayed
 * empty is how a report becomes worthless. So the badge is taken from a RE-PROBE of the metric
 * after the run — the ingest status only survives as the tooltip explaining an absence.
 */

/** A row of `POST /api/earnings/fundamental-coverage`. `served_by` is the canonical ISIN when this
 *  holding is an alias of another (the TSMC ADR → its home line), i.e. the id the matrix keys on. */
export type CoverageRow = {
  isin: string | null; name: string | null; reason: string;
  served_by?: string | null; weight_pct: number;
};

/** A row of `POST /api/earnings/portfolio-revenue-matrix?metric=…`. The value map is called
 *  `revenue` whatever the metric is (the endpoint reuses the key). */
export type MatrixRow = {
  isin: string; name: string; status: string; revenue: Record<string, number | null>;
};

// Reasons a GuruFocus fetch can NEVER close — so no call is spent asking. Mirrors the backend's
// `_fundamental_coverage` reasons; `unsubscribed`/`no_company`/`no_metrics` are deliberately NOT
// here: the ingest resolves an ISIN to its primary SUBSCRIBED listing and can repoint a company
// onto it (Shopify TSX → NASDAQ), so only the fetch itself can say those are hopeless.
const NEVER: Record<string, string> = {
  cash: 'cash — no ISIN to look up',
  not_equity: 'not an equity — a bond / future / FX line has no accounts',
  fund: 'a fund holds companies rather than being one — it has no income statement',
};

/** Does this holding already have the card's metric? A row can be `status: 'ok'` with an empty
 *  map, so the values are what's checked, never the status. */
export const hasMetric = (r: MatrixRow) => Object.values(r.revenue ?? {}).some((v) => v != null);

export type HoldingState = 'present' | 'fetch' | 'never';
export type PlannedHolding = {
  key: string;            // canonical ISIN — what the matrix reports and results are keyed on
  isin: string | null;    // the holding's OWN ISIN (what the ingest is called with)
  name: string;
  state: HoldingState;
  note?: string;          // why it can never have the metric
};
export type Plan = { rows: PlannedHolding[]; queue: PlannedHolding[] };

/**
 * Every distinct holding, in the coverage order (weight, heaviest first), each labelled with what
 * is to be done about it. Deduped on the CANONICAL id (`served_by ?? isin`) — a stock held both
 * directly and inside a linked certificate is ONE fetch, and the ISIN sent is the holding's own
 * (the backend canonicalises it itself).
 */
export function planIngest(coverage: CoverageRow[], matrix: MatrixRow[]): Plan {
  const withData = new Set(matrix.filter(hasMetric).map((r) => r.isin));
  const rows: PlannedHolding[] = [];
  const seen = new Set<string>();

  for (const r of coverage) {
    const name = r.name || r.isin || '—';
    const key = (r.served_by || r.isin || '').trim();
    if (!key) {                                   // a cash line — no ISIN at all
      rows.push({ key: `cash:${name}`, isin: null, name, state: 'never', note: NEVER.cash });
      continue;
    }
    if (seen.has(key)) continue;
    seen.add(key);
    if (withData.has(key)) { rows.push({ key, isin: r.isin, name, state: 'present' }); continue; }
    const note = NEVER[r.reason];
    rows.push(note
      ? { key, isin: r.isin, name, state: 'never', note }
      : { key, isin: r.isin, name, state: 'fetch' });
  }
  return { rows, queue: rows.filter((r) => r.state === 'fetch') };
}

/** One completed attempt: the ingest endpoint's own answer for a holding. */
export type Attempt = { key: string; status?: string; detail?: string };
export type Tone = 'ok' | 'warn' | 'muted' | 'pending';

// Why a holding still has nothing — the tooltip, never the label. Each is an ANSWER, not a fault:
// a fetch that worked and found no such line is not the same as a listing we cannot buy.
const WHY: Record<string, string> = {
  ingested: 'fetched — the company reports none of this line',
  no_data: 'GuruFocus has no fundamentals for this listing',
  unsubscribed: 'exchange outside our GuruFocus subscription',
  not_found: 'the ISIN resolves to no GuruFocus listing',
  not_equity: 'not an equity — no earnings to fetch',
  error: 'the fetch failed',
};

export type Badge = { label: string; tone: Tone; note?: string };

/**
 * What one holding shows after the run: present, or not — with `unsubscribed` called out, since
 * it is the one absence a purchase (not an ingest) would fix. `present` comes from the re-probe,
 * so it means the card can actually chart this holding.
 */
export function badgeFor(row: PlannedHolding, attempt: Attempt | undefined, present: boolean): Badge {
  if (present || row.state === 'present') return { label: 'present', tone: 'ok' };
  if (attempt?.status === 'unsubscribed') {
    return { label: 'unsubscribed', tone: 'warn', note: WHY.unsubscribed };
  }
  if (row.state === 'never') return { label: '—', tone: 'muted', note: row.note };
  if (!attempt) return { label: '…', tone: 'pending' };            // not reached yet
  return {
    label: '—', tone: 'muted',
    note: attempt.detail ? `${WHY[attempt.status ?? ''] ?? attempt.status}: ${attempt.detail}`
      : (WHY[attempt.status ?? ''] ?? attempt.status ?? 'no answer'),
  };
}

/** Headline counts, measured off the re-probe — never off the ingest statuses. */
export function summarize(rows: PlannedHolding[], present: (key: string) => boolean) {
  const n = rows.filter((r) => r.state === 'present' || present(r.key)).length;
  return { total: rows.length, present: n };
}
