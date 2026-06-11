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

export type UseCompanyFiltersResult = ReturnType<typeof useCompanyFilters>;

export function useCompanyFilters(companies: Company[]) {
  const [search, setSearch] = useState('');
  // Multi-select filters. Exchange / Country combine as OR (a company has
  // exactly one of each, so AND would always return empty as soon as 2+
  // are checked). Universe combines as AND so the user can pick the
  // intersection of multiple memberships (e.g. ACWI ∩ LEONTEQ).
  const [filterExchange, setFilterExchange] = useState<string[]>([]);
  const [filterCountry, setFilterCountry] = useState<string[]>([]);
  const [filterSector, setFilterSector] = useState<string[]>([]);
  const [filterUniverse, setFilterUniverse] = useState<string[]>([]);
  const [filterDupes, setFilterDupes] = useState(false);
  const [sortField, setSortField] = useState<SortField>('company_name');
  const [sortDir, setSortDir] = useState<SortDir>('asc');

  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    let list = companies;
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
    if (filterUniverse.length > 0) {
      list = list.filter((c) => {
        const us = c.universes ?? [];
        return filterUniverse.every((u) => us.includes(u));
      });
    }
    if (filterDupes) {
      const nameCounts = new Map<string, number>();
      for (const c of companies) {
        const name = (c.company_name ?? '').trim().toLowerCase();
        if (name) nameCounts.set(name, (nameCounts.get(name) ?? 0) + 1);
      }
      list = list.filter((c) => {
        const name = (c.company_name ?? '').trim().toLowerCase();
        return name && (nameCounts.get(name) ?? 0) > 1;
      });
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
  }, [companies, search, filterExchange, filterCountry, filterSector, filterUniverse, filterDupes, sortField, sortDir]);

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
    setFilterDupes(false);
  }, []);

  const hasActiveFilters =
    !!search || filterExchange.length > 0 || filterCountry.length > 0 || filterSector.length > 0 || filterUniverse.length > 0 || filterDupes;

  return {
    search, setSearch,
    filterExchange, setFilterExchange,
    filterCountry, setFilterCountry,
    filterSector, setFilterSector,
    filterUniverse, setFilterUniverse,
    filterDupes, setFilterDupes,
    sortField, sortDir,
    filtered,
    handleSort,
    toggleUniverse,
    clearFilters,
    hasActiveFilters,
  };
}
