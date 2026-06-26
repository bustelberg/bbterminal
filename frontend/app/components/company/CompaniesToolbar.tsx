'use client';

import { useMemo } from 'react';
import type { Column } from '../../../lib/tableExport';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import TableDownloadButton from '../TableDownloadButton';
import MultiSelectFilter from './MultiSelectFilter';
import type { Company } from './types';
import { NO_UNIVERSE, type UseCompanyFiltersResult } from './useCompanyFilters';

/** The search / multi-select-filter / download bar above the table.
 * Filter state comes from `useCompanyFilters`; the export columns mirror
 * the visible data columns (Actions is UI-only and skipped). */
export default function CompaniesToolbar({
  filters,
  exchangeOptions,
  countryOptions,
  sectorOptions,
  universeOptions,
  openfigiOptions,
  rows,
}: {
  filters: UseCompanyFiltersResult;
  exchangeOptions: string[];
  countryOptions: string[];
  sectorOptions: string[];
  universeOptions: string[];
  openfigiOptions: string[];
  rows: Company[];
}) {
  const {
    search, setSearch,
    filterExchange, setFilterExchange,
    filterCountry, setFilterCountry,
    filterSector, setFilterSector,
    filterUniverse, setFilterUniverse,
    filterOpenfigi, setFilterOpenfigi,
    filterDupes, setFilterDupes,
    clearFilters, hasActiveFilters,
  } = filters;

  // Every company field is exported by default; the download picker lets the
  // user untick any they don't want. Numbers stay numeric (Excel treats them
  // as numbers, not text); nulls render blank.
  const exportColumns = useMemo<Column<Company>[]>(() => [
    { key: 'company_id', header: 'ID', accessor: (c) => c.company_id },
    { key: 'company_name', header: 'Name', accessor: (c) => c.company_name ?? '' },
    { key: 'gurufocus_ticker', header: 'Ticker', accessor: (c) => c.gurufocus_ticker },
    { key: 'gurufocus_exchange', header: 'Exchange', accessor: (c) => c.gurufocus_exchange },
    { key: 'isin', header: 'ISIN', accessor: (c) => c.isin ?? '' },
    { key: 'country', header: 'Country', accessor: (c) => c.country ?? '' },
    { key: 'sector', header: 'Sector', accessor: (c) => c.sector ?? '' },
    { key: 'sector_source', header: 'Sector source', accessor: (c) => c.sector_source ?? '' },
    { key: 'universes', header: 'Memberships', accessor: (c) => (c.universes ?? []).join(' | ') },
    { key: 'market_cap_eur', header: 'Market cap (EUR)', accessor: (c) => c.market_cap_eur ?? null },
    { key: 'market_cap_native', header: 'Market cap (native)', accessor: (c) => c.market_cap_native ?? null },
    { key: 'market_cap_currency', header: 'Market cap currency', accessor: (c) => c.market_cap_currency ?? '' },
    { key: 'market_cap_fx_rate', header: 'FX rate (per EUR)', accessor: (c) => c.market_cap_fx_rate ?? null },
    { key: 'market_cap_date', header: 'Market cap date', accessor: (c) => c.market_cap_date ?? '' },
    { key: 'gf_unsubscribed', header: 'GF unsubscribed', accessor: (c) => (c.gf_unsubscribed ? 'yes' : '') },
    { key: 'openfigi_status', header: 'OpenFIGI status', accessor: (c) => c.openfigi_status ?? '' },
    { key: 'openfigi_name', header: 'OpenFIGI name', accessor: (c) => c.openfigi_name ?? '' },
    { key: 'openfigi_checked_at', header: 'OpenFIGI checked', accessor: (c) => c.openfigi_checked_at ?? '' },
    { key: 'delisted_at', header: 'Delisted at', accessor: (c) => c.delisted_at ?? '' },
    { key: 'out_of_scope_at', header: 'Out of scope at', accessor: (c) => c.out_of_scope_at ?? '' },
    { key: 'out_of_scope_reason', header: 'Out of scope reason', accessor: (c) => c.out_of_scope_reason ?? '' },
    { key: 'gurufocus_lookup_failed_at', header: 'GF lookup failed at', accessor: (c) => c.gurufocus_lookup_failed_at ?? '' },
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
        options={[NO_UNIVERSE, ...universeOptions]}
        selected={filterUniverse}
        onChange={setFilterUniverse}
        combineMode="AND"
      />
      <MultiSelectFilter
        label="OpenFIGI"
        options={openfigiOptions}
        selected={filterOpenfigi}
        onChange={setFilterOpenfigi}
        combineMode="OR"
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
          confirmNoun="companies"
        />
      </div>
    </div>
  );
}
