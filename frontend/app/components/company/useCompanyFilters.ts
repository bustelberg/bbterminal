/**
 * `useCompanyFilters` — pure view state for the `/companies` table:
 * the search box, the three multi-select filters (+ the duplicates
 * toggle), the sort field/direction, and the derived `filtered` list.
 *
 * Split from the data/mutation concern (`useCompanies`) so each table-
 * filtering change is testable in isolation and the orchestrator just
 * threads the result into the toolbar + table.
 */
import { useCallback, useMemo, useState } from 'react';
import type { Company, SortField, SortDir } from './types';
import { computeNameDupes } from './nameDupe';

/** Sentinel option in the Universe filter for "companies in no universe".
 * Unlikely to collide with a real label (all of which are "X (as of …)"). */
export const NO_UNIVERSE = '(No universe)';

/** Sentinel option in the OpenFIGI filter for "not yet verified" (null status). */
export const OPENFIGI_UNCHECKED = '(Unchecked)';

export type UseCompanyFiltersResult = ReturnType<typeof useCompanyFilters>;

export function useCompanyFilters(companies: Company[], membershipsLoading = false) {
  const [search, setSearch] = useState('');
  // Companies that belong to no (frozen) universe are KEPT and shown with a
  // "No membership" badge (the user wants them visible, not hidden). Default
  // OFF — they're surfaced by default; the "N not in any universe" header pill
  // still filters TO only them. (Nameless empty stubs are filtered out below
  // regardless, via the `!!c.company_name` guard.)
  const [hideUnlinked, setHideUnlinked] = useState(false);
  // Multi-select filters. Exchange / Country combine as OR (a company has
  // exactly one of each, so AND would always return empty as soon as 2+
  // are checked). Universe combines as AND so the user can pick the
  // intersection of multiple memberships (e.g. ACWI ∩ LEONTEQ).
  const [filterExchange, setFilterExchange] = useState<string[]>([]);
  const [filterCountry, setFilterCountry] = useState<string[]>([]);
  const [filterSector, setFilterSector] = useState<string[]>([]);
  const [filterUniverse, setFilterUniverse] = useState<string[]>([]);
  // OpenFIGI verification status — OR multi-select (a row has one status).
  // The `OPENFIGI_UNCHECKED` sentinel matches rows with a null status.
  const [filterOpenfigi, setFilterOpenfigi] = useState<string[]>([]);
  const [filterDupes, setFilterDupes] = useState(false);
  const [filterNameDupes, setFilterNameDupes] = useState(false);
  const [filterGfLookup, setFilterGfLookup] = useState(false);
  const [sortField, setSortField] = useState<SortField>('company_name');
  const [sortDir, setSortDir] = useState<SortDir>('asc');

  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    let list = companies;
    // Explicitly filtering FOR the no-universe companies overrides hideUnlinked.
    const wantNoUniverse = filterUniverse.includes(NO_UNIVERSE);
    // A status filter (dupes / name dupes / GF lookup) should surface ALL its
    // matches — including unlinked stubs, which are often exactly what you want
    // to clean up — so it bypasses the default hide-unlinked.
    const statusFilterActive = filterDupes || filterNameDupes || filterGfLookup;
    // Nameless empty stubs are ALWAYS dropped (pure junk — no name in the base
    // response), regardless of hideUnlinked, so they never flash in.
    list = list.filter((c) => !!c.company_name);
    if (hideUnlinked && !wantNoUniverse && !statusFilterActive) {
      // Optional: also hide NAMED companies in no (frozen) universe. Default off
      // now — those show with a "No membership" badge — but the toggle is kept.
      list = list.filter(
        (c) => membershipsLoading || (c.universes ?? []).length > 0,
      );
    }
    if (q) {
      list = list.filter(
        (c) =>
          (c.company_name ?? '').toLowerCase().includes(q) ||
          c.gurufocus_ticker.toLowerCase().includes(q) ||
          c.gurufocus_exchange.toLowerCase().includes(q),
      );
    }
    if (filterExchange.length > 0) {
      list = list.filter((c) => filterExchange.includes(c.gurufocus_exchange));
    }
    if (filterCountry.length > 0) {
      list = list.filter((c) => c.country != null && filterCountry.includes(c.country));
    }
    if (filterSector.length > 0) {
      list = list.filter((c) => c.sector != null && filterSector.includes(c.sector));
    }
    if (filterOpenfigi.length > 0) {
      list = list.filter((c) => filterOpenfigi.includes(c.openfigi_status ?? OPENFIGI_UNCHECKED));
    }
    if (filterUniverse.length > 0) {
      // "(No universe)" matches companies with zero memberships. Real labels
      // still AND together (the intersection). Picking the sentinel alongside
      // real labels unions the two (no-universe rows OR rows in all picked
      // universes).
      const realLabels = filterUniverse.filter((u) => u !== NO_UNIVERSE);
      list = list.filter((c) => {
        const us = c.universes ?? [];
        const matchesNone = wantNoUniverse && us.length === 0;
        if (realLabels.length === 0) return matchesNone;
        return matchesNone || realLabels.every((u) => us.includes(u));
      });
    }
    if (filterDupes) {
      // True duplicates = same ISIN (same security), not same name.
      const isinCounts = new Map<string, number>();
      for (const c of companies) {
        const isin = (c.isin ?? '').trim();
        if (isin) isinCounts.set(isin, (isinCounts.get(isin) ?? 0) + 1);
      }
      list = list.filter((c) => {
        const isin = (c.isin ?? '').trim();
        return isin && (isinCounts.get(isin) ?? 0) > 1;
      });
    }
    if (filterNameDupes) {
      // Same-name companies where at least one side has no ISIN — the gap the
      // ISIN dupe filter can't see.
      const ids = computeNameDupes(companies);
      list = list.filter((c) => ids.has(c.company_id));
    }
    if (filterGfLookup) {
      // Match the GF LOOKUP badge: delisted / out-of-scope rows show those
      // badges instead, so they're not "GF lookup" for filtering purposes.
      list = list.filter((c) => !!c.gurufocus_lookup_failed_at && !c.delisted_at && !c.out_of_scope_at);
    }

    return [...list].sort((a, b) => {
      // Market cap sorts numerically; nulls sink to the bottom regardless of
      // direction so "no data" never floats to the top of a desc sort.
      if (sortField === 'market_cap_eur') {
        const av = a.market_cap_eur;
        const bv = b.market_cap_eur;
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        return sortDir === 'asc' ? av - bv : bv - av;
      }
      const av = (a[sortField] ?? '') as string;
      const bv = (b[sortField] ?? '') as string;
      const cmp = av.localeCompare(bv, undefined, { sensitivity: 'base' });
      return sortDir === 'asc' ? cmp : -cmp;
    });
  }, [companies, hideUnlinked, membershipsLoading, search, filterExchange, filterCountry, filterSector, filterUniverse, filterOpenfigi, filterDupes, filterNameDupes, filterGfLookup, sortField, sortDir]);

  const handleSort = useCallback((field: SortField) => {
    if (sortField === field) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortField(field);
      setSortDir('asc');
    }
  }, [sortField]);

  const toggleUniverse = useCallback((u: string) => {
    setFilterUniverse((cur) => (cur.includes(u) ? cur.filter((x) => x !== u) : [...cur, u]));
  }, []);

  const clearFilters = useCallback(() => {
    setSearch('');
    setFilterExchange([]);
    setFilterCountry([]);
    setFilterSector([]);
    setFilterUniverse([]);
    setFilterOpenfigi([]);
    setFilterDupes(false);
    setFilterNameDupes(false);
    setFilterGfLookup(false);
  }, []);

  const hasActiveFilters =
    !!search || filterExchange.length > 0 || filterCountry.length > 0 || filterSector.length > 0 || filterUniverse.length > 0 || filterOpenfigi.length > 0 || filterDupes || filterNameDupes || filterGfLookup;

  return {
    search, setSearch,
    filterExchange, setFilterExchange,
    filterCountry, setFilterCountry,
    filterSector, setFilterSector,
    filterUniverse, setFilterUniverse,
    filterOpenfigi, setFilterOpenfigi,
    filterDupes, setFilterDupes,
    filterNameDupes, setFilterNameDupes,
    filterGfLookup, setFilterGfLookup,
    hideUnlinked, setHideUnlinked,
    sortField, sortDir,
    filtered,
    handleSort,
    toggleUniverse,
    clearFilters,
    hasActiveFilters,
  };
}
