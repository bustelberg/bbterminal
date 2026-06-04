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
  universeOptions,
  rows,
}: {
  filters: UseCompanyFiltersResult;
  exchangeOptions: string[];
  countryOptions: string[];
  universeOptions: string[];
  rows: Company[];
}) {
  const {
    search, setSearch,
    filterExchange, setFilterExchange,
    filterCountry, setFilterCountry,
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
    { key: 'universes', header: 'Memberships', accessor: (c) => (c.universes ?? []).join(' | ') },
    { key: 'gurufocus_url', header: 'GuruFocus URL', accessor: (c) => guruFocusUrl(c.gurufocus_ticker, c.gurufocus_exchange) },
  ], []);

  return (
    <div className="px-8 py-3 border-b border-gray-800/60 flex items-center gap-3 flex-wrap">
      <input
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        placeholder="Search name, ticker, exchange..."
        className="bg-[#151821] border border-gray-800/60 rounded-lg px-3 py-2 text-sm text-white w-72 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 placeholder-gray-600 transition-colors"
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
            ? 'bg-rose-500/20 border border-rose-500/40 text-rose-400'
            : 'bg-[#151821] border border-gray-800/60 text-gray-400 hover:text-white'
        }`}
      >
        Duplicates
      </button>
      {hasActiveFilters && (
        <button
          onClick={clearFilters}
          className="text-sm text-gray-500 hover:text-white transition-colors"
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
