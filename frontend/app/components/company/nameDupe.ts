import type { Company } from './types';

/**
 * Name-based duplicate detection — the complement to the ISIN-based DUPE badge.
 *
 * The DUPE badge keys on a *shared ISIN*, so it can't catch a row that has no
 * ISIN duplicating one that does (e.g. the LongEquity "Celestica" stub
 * TSX:CLA, no ISIN, vs the Leonteq "Celestica Inc" TSX:CLS with one). This
 * flags companies that share a *name* where at least one side lacks an ISIN.
 *
 * The no-ISIN condition is deliberate: it keeps legitimate same-name share
 * classes (GOOG/GOOGL → "alphabet") from flagging, since those both carry
 * (different) ISINs. We only strip trailing CORPORATE-FORM suffixes, never
 * meaningful words — so "BYD Co Ltd" → "byd" and "BYD Electronic" →
 * "byd electronic" stay distinct.
 */

const CORP_SUFFIXES = new Set([
  'inc', 'incorporated', 'ltd', 'limited', 'corp', 'corporation', 'co', 'company',
  'plc', 'sa', 'ag', 'nv', 'spa', 'se', 'llc', 'lp', 'group', 'holding', 'holdings',
  'adr', 'ads',
]);

function nameTokens(name: string | null | undefined): string[] {
  return (name ?? '').toLowerCase().replace(/[^a-z0-9 ]+/g, ' ').split(/\s+/).filter(Boolean);
}

/** Coarse bucket key: lowercase name with trailing corporate-suffix tokens
 * stripped, so "Celestica" and "Celestica Inc" bucket together. `foldable`
 * makes the final, safe decision. */
export function nameDupeKey(name: string | null | undefined): string {
  const toks = nameTokens(name);
  while (toks.length > 1 && CORP_SUFFIXES.has(toks[toks.length - 1])) toks.pop();
  return toks.join(' ');
}

/** True iff one name is the other with ONLY trailing corporate-suffix tokens
 * added (or identical): "Celestica" folds into "Celestica Inc"; but "Siemens
 * Ltd" (India) ≠ "Siemens AG" (parent), and "Apple" ≠ "Apple Hospitality".
 * Mirrors `_foldable_names` in backend/ingest/dedupe.py. */
function foldable(a: string[], b: string[]): boolean {
  const [short, long] = a.length <= b.length ? [a, b] : [b, a];
  if (!short.length) return false;
  for (let i = 0; i < short.length; i++) if (long[i] !== short[i]) return false;
  return long.slice(short.length).every((t) => CORP_SUFFIXES.has(t));
}

/** `company_id → the other company(ies) it duplicates by name`, for a name
 * bucket holding EXACTLY ONE ISIN-bearing company plus one-or-more no-ISIN
 * stubs whose name folds into it (same rule the backend auto-merge uses, so
 * the badge shows exactly what prevention will clean up). Empty when none. */
export function computeNameDupes(companies: Company[]): Map<number, Company[]> {
  const byKey = new Map<string, Company[]>();
  for (const c of companies) {
    const k = nameDupeKey(c.company_name);
    if (!k) continue;
    const g = byKey.get(k);
    if (g) g.push(c);
    else byKey.set(k, [c]);
  }
  const out = new Map<number, Company[]>();
  for (const g of byKey.values()) {
    if (g.length < 2) continue;
    const withIsin = g.filter((c) => (c.isin ?? '').trim());
    const noIsin = g.filter((c) => !(c.isin ?? '').trim());
    if (withIsin.length !== 1 || !noIsin.length) continue; // one canonical + stubs
    const winner = withIsin[0];
    const wTok = nameTokens(winner.company_name);
    const fold = noIsin.filter((c) => foldable(wTok, nameTokens(c.company_name)));
    if (!fold.length) continue;
    const grp = [winner, ...fold];
    for (const c of grp) out.set(c.company_id, grp.filter((o) => o.company_id !== c.company_id));
  }
  return out;
}
