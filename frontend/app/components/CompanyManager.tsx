'use client';

import { useState } from 'react';
import { useIsAdmin } from '../../lib/hooks/useEffectiveRole';
import LoadingDots from './LoadingDots';
import CompaniesToolbar from './company/CompaniesToolbar';
import CompanyTable from './company/CompanyTable';
import MarketCapRefreshButton from './company/MarketCapRefreshButton';
import VerifyAddModal from './company/VerifyAddModal';
import { useCompanies } from './company/useCompanies';
import { useCompanyFilters } from './company/useCompanyFilters';

// This page was decomposed (2026-06-04) into `app/components/company/`:
// data fetching + Add/Edit/Delete mutations live in `useCompanies`, the
// search/filter/sort view state in `useCompanyFilters`, shared shapes in
// `types.ts`, and each render piece (toolbar, table, rows, modal) is its
// own component. When extending /companies, add/extend a hook or a section
// component — don't regrow this orchestrator. It owns only the small UI
// toggles (adding / editingId) that span the header + table.

export default function CompanyManager() {
  const data = useCompanies();
  const filters = useCompanyFilters(data.companies);
  // Mutation controls (Add / Edit / Delete) are admin-only. Read paths
  // — sort, search, filters, universe chips — stay open to everyone.
  const isAdmin = useIsAdmin();

  const [editingId, setEditingId] = useState<number | null>(null);
  const [adding, setAdding] = useState(false);

  const { companies, loading, error, setError, duplicateCount } = data;
  const { filtered, filterDupes, setFilterDupes } = filters;

  return (
    <div className="flex flex-col h-full">
      <div className="px-8 py-5 border-b border-neutral-800/60 flex items-center justify-between gap-4">
        <div>
          <h1 className="text-lg font-semibold text-fg-strong">Companies</h1>
          <p className="text-xs text-fg-subtle mt-0.5">
            {loading ? <LoadingDots label="Loading" /> : `${filtered.length} of ${companies.length} companies`}
            {!loading && duplicateCount > 0 && (
              <>
                {' · '}
                <button
                  onClick={() => setFilterDupes(!filterDupes)}
                  className={`underline-offset-2 hover:underline transition-colors ${
                    filterDupes ? 'text-neg-400' : 'text-neg-400/80 hover:text-neg-400'
                  }`}
                  title={filterDupes ? 'Click to clear duplicates filter' : 'Click to show only duplicate entries'}
                >
                  {duplicateCount} duplicate{duplicateCount === 1 ? '' : 's'}
                </button>
              </>
            )}
          </p>
        </div>
        {isAdmin && (
          <div className="flex items-center gap-2">
            <MarketCapRefreshButton onRefreshed={data.load} />
            <button
              onClick={() => { setAdding(true); setEditingId(null); }}
              className="px-4 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors"
            >
              + Add company
            </button>
          </div>
        )}
      </div>

      <CompaniesToolbar
        filters={filters}
        exchangeOptions={data.exchangeOptions}
        countryOptions={data.countryOptions}
        sectorOptions={data.sectorOptions}
        universeOptions={data.universeOptions}
        rows={filtered}
      />

      {error && (
        <div className="mx-8 mt-4 px-4 py-3 text-sm text-neg-400 bg-neg-500/10 border border-neg-500/20 rounded-lg flex items-center justify-between">
          <span>{error}</span>
          <button onClick={() => setError(null)} className="text-fg-subtle hover:text-fg-strong ml-3 text-xs">Dismiss</button>
        </div>
      )}

      <CompanyTable
        rows={filtered}
        totalCount={companies.length}
        loading={loading}
        membershipsLoading={data.membershipsLoading}
        isAdmin={isAdmin}
        adding={adding}
        editingId={editingId}
        exchangeOptions={data.exchangeOptions}
        duplicateNames={data.duplicateNames}
        deletingId={data.deletingId}
        sortField={filters.sortField}
        sortDir={filters.sortDir}
        onSort={filters.handleSort}
        onAdd={data.handleAdd}
        onCancelAdd={() => setAdding(false)}
        onSave={async (id, updated) => { if (await data.handleSave(id, updated)) setEditingId(null); }}
        onEdit={(id) => { setEditingId(id); setAdding(false); }}
        onCancelEdit={() => setEditingId(null)}
        onDelete={data.handleDelete}
        onFindExchange={data.findCorrectExchange}
        onToggleUniverse={filters.toggleUniverse}
      />

      {data.pendingAdd && (
        <VerifyAddModal
          pendingAdd={data.pendingAdd}
          confirming={data.confirming}
          onConfirm={async () => { if (await data.confirmAdd()) setAdding(false); }}
          onCancel={() => data.setPendingAdd(null)}
        />
      )}
    </div>
  );
}
