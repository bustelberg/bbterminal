'use client';

import { useCallback, useMemo, useState } from 'react';
import { useIsAdmin } from '../../lib/hooks/useEffectiveRole';
import LoadingDots from './LoadingDots';
import CompaniesToolbar from './company/CompaniesToolbar';
import CompanyTable from './company/CompanyTable';
import MarketCapRefreshButton from './company/MarketCapRefreshButton';
import OpenFigiVerifyButton from './company/OpenFigiVerifyButton';
import VerifyAddModal from './company/VerifyAddModal';
import { useCompanies } from './company/useCompanies';
import { useCompanyFilters, NO_UNIVERSE } from './company/useCompanyFilters';
import { buildUniverseStyles, FALLBACK_STYLE } from './company/styles';

// This page was decomposed (2026-06-04) into `app/components/company/`:
// data fetching + Add/Edit/Delete mutations live in `useCompanies`, the
// search/filter/sort view state in `useCompanyFilters`, shared shapes in
// `types.ts`, and each render piece (toolbar, table, rows, modal) is its
// own component. When extending /companies, add/extend a hook or a section
// component — don't regrow this orchestrator. It owns only the small UI
// toggles (adding / editingId) that span the header + table.

/** Shared styling for the header filter toggles — a clear pressed (filled)
 * vs unpressed (outline) state so they read as on/off buttons, not links. */
function togglePillCls(active: boolean, tone: 'neg' | 'warn'): string {
  const base = 'px-2 py-0.5 rounded-md border text-xs font-medium transition-colors';
  const tones = {
    neg: active
      ? 'bg-neg-500/15 border-neg-500/40 text-neg-300'
      : 'bg-card border-neutral-800/60 text-neg-400/80 hover:border-neg-500/40 hover:text-neg-300',
    warn: active
      ? 'bg-warn-500/15 border-warn-500/40 text-warn-300'
      : 'bg-card border-neutral-800/60 text-warn-400/80 hover:border-warn-500/40 hover:text-warn-300',
  } as const;
  return `${base} ${tones[tone]}`;
}

export default function CompanyManager() {
  const data = useCompanies();
  const filters = useCompanyFilters(data.companies, data.membershipsLoading);
  // Mutation controls (Add / Edit / Delete) are admin-only. Read paths
  // — sort, search, filters, universe chips — stay open to everyone.
  const isAdmin = useIsAdmin();

  const [editingId, setEditingId] = useState<number | null>(null);
  const [adding, setAdding] = useState(false);

  const { companies, loading, error, setError, duplicateCount, nameDupeCount, gfLookupCount, unlinkedCount } = data;
  const { filtered, filterDupes, setFilterDupes, filterNameDupes, setFilterNameDupes, filterGfLookup, setFilterGfLookup, filterUniverse, toggleUniverse } = filters;
  const showOnlyUnlinked = filterUniverse.includes(NO_UNIVERSE);

  // One distinct colour per universe label, spread evenly around the wheel so
  // chips (and the sector-source annotation) are easy to tell apart. Shared by
  // every row via `universeStyle`.
  const universeStyles = useMemo(() => buildUniverseStyles(data.universeOptions), [data.universeOptions]);
  const universeStyle = useCallback(
    (label: string) => universeStyles.get(label) ?? FALLBACK_STYLE,
    [universeStyles],
  );

  return (
    <div className="flex flex-col h-full">
      <div className="px-8 py-5 border-b border-neutral-800/60 flex items-center justify-between gap-4">
        <div>
          <h1 className="text-lg font-semibold text-fg-strong">Companies</h1>
          <div className="text-xs text-fg-subtle mt-1 flex items-center flex-wrap gap-2">
            <span>{loading ? <LoadingDots label="Loading" /> : `${filtered.length} of ${companies.length} companies`}</span>
            {!loading && duplicateCount > 0 && (
              <button
                type="button"
                aria-pressed={filterDupes}
                onClick={() => setFilterDupes(!filterDupes)}
                className={togglePillCls(filterDupes, 'neg')}
                title={filterDupes ? 'Showing only ISIN duplicates — click to clear' : 'Show only rows sharing an ISIN (same security stored twice)'}
              >
                {duplicateCount} duplicate{duplicateCount === 1 ? '' : 's'} (same ISIN)
              </button>
            )}
            {!loading && nameDupeCount > 0 && (
              <button
                type="button"
                aria-pressed={filterNameDupes}
                onClick={() => setFilterNameDupes(!filterNameDupes)}
                className={togglePillCls(filterNameDupes, 'warn')}
                title={filterNameDupes ? 'Showing only name duplicates — click to clear' : 'Show only same-name companies where one side has no ISIN (the dupes the ISIN check misses, e.g. Celestica)'}
              >
                {nameDupeCount} name dupe{nameDupeCount === 1 ? '' : 's'} (no ISIN)
              </button>
            )}
            {!loading && gfLookupCount > 0 && (
              <button
                type="button"
                aria-pressed={filterGfLookup}
                onClick={() => setFilterGfLookup(!filterGfLookup)}
                className={togglePillCls(filterGfLookup, 'neg')}
                title={filterGfLookup ? 'Showing only GF-lookup-failed companies — click to clear' : 'Show only companies GuruFocus returned "stock not found" for (wrong/retired ticker or exchange)'}
              >
                {gfLookupCount} GF lookup
              </button>
            )}
            {!loading && unlinkedCount > 0 && (
              <button
                type="button"
                aria-pressed={showOnlyUnlinked}
                onClick={() => toggleUniverse(NO_UNIVERSE)}
                className={togglePillCls(showOnlyUnlinked, 'warn')}
                title={showOnlyUnlinked ? 'Showing only companies in no universe — click to clear' : `Show only the ${unlinkedCount} companies in no universe (orphaned stubs)`}
              >
                {unlinkedCount} not in any universe
              </button>
            )}
          </div>
        </div>
        {isAdmin && (
          <div className="flex items-center gap-2">
            <MarketCapRefreshButton onRefreshed={data.load} />
            <OpenFigiVerifyButton onVerified={data.load} />
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
        openfigiOptions={data.openfigiOptions}
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
        sectorsLoading={data.sectorsLoading}
        isAdmin={isAdmin}
        adding={adding}
        editingId={editingId}
        exchangeOptions={data.exchangeOptions}
        duplicateIsins={data.duplicateIsins}
        nameDupes={data.nameDupes}
        deletingId={data.deletingId}
        verifyingId={data.verifyingId}
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
        onFetchGfName={data.fetchGfName}
        onVerifyOpenfigi={data.verifyOne}
        onToggleUniverse={filters.toggleUniverse}
        universeStyle={universeStyle}
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
