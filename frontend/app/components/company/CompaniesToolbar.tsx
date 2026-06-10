'use client';

import { useMemo } from 'react';
import type { Column } from '../../../lib/tableExport';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import TableDownloadButton from '../TableDownloadButton';
import MultiSelectFilter from './MultiSelectFilter';
import type { Company } from './types';
import type { UseCompanyFiltersResult } from './useCompanyFilters';

/** The search / multi-select-filter / download bar above the table.
 * Filter state comes from `useCompanyFilters`; the export columns mirror
 * the visible data columns (Actions is UI-only and skipped). */
export default function CompaniesToolbar({
  filters,
  exchangeOptions,
  countryOptions,
  sectorOptions,
  universeOptions,
  rows,
}: {
  filters: UseCompanyFiltersResult;
  exchangeOptions: string[];
  countryOptions: string[];
  sectorOptions: string[];
  universeOptions: string[];
  rows: Company[];
}) {
  const {
    search, setSearch,
    filterExchange, setFilterExchange,
    filterCountry, setFilterCountry,
    filterSector, setFilterSector,
    filterUniverse, setFilterUniverse,
    filterDupes, setFilterDupes,
    clearFilters, hasActiveFilters,
  } = filters;

  const exportColumns = useMemo<Column<Company>[]>(() => [
    { key: 'company_id', header: 'ID', accessor: (c) => c.company_id },
    { key: 'company_name', header: 'Name', accessor: (c) => c.company_name ?? '' },
    { key: 'gurufocus_ticker', header: 'Ticker', accessor: (c) => c.gurufocus_ticker },
    { key: 'gurufocus_exchange', header: 'Exchange', accessor: (c) => c.gurufocus_exchange },
    { key: 'country', header: 'Country', accessor: (c) => c.country ?? '' },
    { key: 'sector', header: 'Sector', accessor: (c) => c.sector ?? '' },
    { key: 'universes', header: 'Memberships', accessor: (c) => (c.universes ?? []).join(' | ') },
    { key: 'gurufocus_url', header: 'GuruFocus URL', accessor: (c) => guruFocusUrl(c.gurufocus_ticker, c.gurufocus_exchange) },
  ], []);

  return (
    <div className="px-8 py-3 border-b border-neutral-800/60 flex items-center gap-3 flex-wrap">
      <input
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        placeholder="Search name, ticker, exchange..."
        className="bg-card border border-neutral-800/60 rounded-lg px-3 py-2 text-sm text-fg-strong w-72 focus:outline-none focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 placeholder-fg-faint transition-colors"
      />
      <MultiSelectFilter
        label="Exchanges"
        options={exchangeOptions}
        selected={filterExchange}
        onChange={setFilterExchange}
        combineMode="OR"
      />
      <MultiSelectFilter
        label="Countries"
        options={countryOptions}
        selected={filterCountry}
        onChange={setFilterCountry}
        combineMode="OR"
      />
      <MultiSelectFilter
        label="Sectors"
        options={sectorOptions}
        selected={filterSector}
        onChange={setFilterSector}
        combineMode="OR"
      />
      <MultiSelectFilter
        label="Universes"
        options={universeOptions}
        selected={filterUniverse}
        onChange={setFilterUniverse}
        combineMode="AND"
      />
      <button
        onClick={() => setFilterDupes(!filterDupes)}
        className={`px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
          filterDupes
            ? 'bg-neg-500/20 border border-neg-500/40 text-neg-400'
            : 'bg-card border border-neutral-800/60 text-fg-muted hover:text-fg-strong'
        }`}
      >
        Duplicates
      </button>
      {hasActiveFilters && (
        <button
          onClick={clearFilters}
          className="text-sm text-fg-subtle hover:text-fg-strong transition-colors"
        >
          Clear filters
        </button>
      )}
      <div className="ml-auto">
        <TableDownloadButton
          rows={rows}
          columns={exportColumns}
          filename="companies"
          title={`Download ${rows.length} companies as CSV / XLSX`}
        />
      </div>
    </div>
  );
}
