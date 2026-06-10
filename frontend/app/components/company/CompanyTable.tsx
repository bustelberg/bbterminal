'use client';

import Spinner from '../Spinner';
import AddRow from './AddRow';
import EditRow from './EditRow';
import CompanyRow from './CompanyRow';
import { thCls } from './styles';
import type { Company, SortField } from './types';

const sortIcon = (sortField: SortField, sortDir: 'asc' | 'desc', field: SortField) => {
  if (sortField !== field) return '';
  return sortDir === 'asc' ? ' ▴' : ' ▾';
};

/** The companies table — header (sortable, with the name-dupe badge),
 * optional add row, and one editing-or-static row per filtered company.
 * All state lives in the parent hooks; this component is the render shell
 * wiring the row sub-components to the handlers. */
export default function CompanyTable({
  rows,
  totalCount,
  loading,
  membershipsLoading,
  isAdmin,
  adding,
  editingId,
  exchangeOptions,
  duplicateNames,
  deletingId,
  sortField,
  sortDir,
  onSort,
  onAdd,
  onCancelAdd,
  onSave,
  onEdit,
  onCancelEdit,
  onDelete,
  onFindExchange,
  onToggleUniverse,
}: {
  rows: Company[];
  totalCount: number;
  loading: boolean;
  membershipsLoading: boolean;
  isAdmin: boolean;
  adding: boolean;
  editingId: number | null;
  exchangeOptions: string[];
  duplicateNames: Set<string>;
  deletingId: number | null;
  sortField: SortField;
  sortDir: 'asc' | 'desc';
  onSort: (field: SortField) => void;
  onAdd: (c: { company_name: string; gurufocus_ticker: string; gurufocus_exchange: string }) => Promise<void>;
  onCancelAdd: () => void;
  onSave: (id: number, updated: Partial<Company>) => Promise<void>;
  onEdit: (id: number) => void;
  onCancelEdit: () => void;
  onDelete: (id: number, name: string) => void;
  onFindExchange: (c: Company) => void;
  onToggleUniverse: (u: string) => void;
}) {
  return (
    <div className="flex-1 overflow-auto px-8 py-4">
      <div className="bg-card rounded-xl border border-neutral-800/40 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-neutral-800/60 text-fg-subtle">
              <th className="px-4 py-3 text-left text-xs font-medium w-12">ID</th>
              <th className={thCls} onClick={() => onSort('company_name')}>
                <span className="flex items-center gap-2">
                  Name{sortIcon(sortField, sortDir, 'company_name')}
                  {!loading && (
                    duplicateNames.size > 0 ? (
                      <span className="px-1.5 py-0.5 text-[10px] font-medium bg-warn-500/15 text-warn-400 border border-warn-500/25 rounded">
                        {duplicateNames.size} dupe{duplicateNames.size > 1 ? 's' : ''}
                      </span>
                    ) : (
                      <span className="px-1.5 py-0.5 text-[10px] font-medium bg-pos-500/15 text-pos-400 border border-pos-500/25 rounded">
                        no dupes
                      </span>
                    )
                  )}
                </span>
              </th>
              <th className={`${thCls} w-24`} onClick={() => onSort('gurufocus_ticker')}>Ticker{sortIcon(sortField, sortDir, 'gurufocus_ticker')}</th>
              <th className={`${thCls} w-24`} onClick={() => onSort('gurufocus_exchange')}>Exchange{sortIcon(sortField, sortDir, 'gurufocus_exchange')}</th>
              <th className={`${thCls} w-36`} onClick={() => onSort('isin')}>ISIN{sortIcon(sortField, sortDir, 'isin')}</th>
              <th className={`${thCls} w-32`} onClick={() => onSort('country')}>Country{sortIcon(sortField, sortDir, 'country')}</th>
              <th className="px-3 py-3 text-left text-xs font-medium">Memberships</th>
              <th className="px-3 py-3 text-left text-xs font-medium w-28">Actions</th>
            </tr>
          </thead>
          <tbody>
            {adding && (
              <AddRow
                exchangeOptions={exchangeOptions}
                onAdd={onAdd}
                onCancel={onCancelAdd}
              />
            )}
            {loading && (
              <tr>
                <td colSpan={8} className="py-14 text-center">
                  <span className="inline-flex items-center gap-2.5 text-fg-subtle text-sm">
                    <Spinner size={14} />
                    <span>Loading companies…</span>
                  </span>
                </td>
              </tr>
            )}
            {rows.map((c) =>
              editingId === c.company_id ? (
                <EditRow
                  key={c.company_id}
                  company={c}
                  exchangeOptions={exchangeOptions}
                  onSave={(updated) => onSave(c.company_id, updated)}
                  onCancel={onCancelEdit}
                />
              ) : (
                <CompanyRow
                  key={c.company_id}
                  company={c}
                  isAdmin={isAdmin}
                  membershipsLoading={membershipsLoading}
                  duplicateNames={duplicateNames}
                  deletingId={deletingId}
                  onEdit={onEdit}
                  onDelete={onDelete}
                  onFindExchange={onFindExchange}
                  onToggleUniverse={onToggleUniverse}
                />
              ),
            )}
          </tbody>
        </table>
      </div>
      {!loading && rows.length === 0 && (
        <p className="text-center text-fg-subtle text-sm py-12">
          {totalCount === 0 ? 'No companies in database.' : 'No companies match your filters.'}
        </p>
      )}
    </div>
  );
}
