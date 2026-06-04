'use client';

import Spinner from '../Spinner';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import type { PendingAdd } from './types';

/** Confirmation modal shown before a company is POSTed. Forces the user to
 * eyeball the GuruFocus URL — a wrong ticker/exchange pair means no price
 * data will ever load for the row. */
export default function VerifyAddModal({
  pendingAdd,
  confirming,
  onConfirm,
  onCancel,
}: {
  pendingAdd: PendingAdd;
  confirming: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  const url = guruFocusUrl(pendingAdd.gurufocus_ticker, pendingAdd.gurufocus_exchange);
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
      <div className="bg-[#151821] border border-gray-800/60 rounded-xl p-6 max-w-lg w-full mx-4 shadow-xl">
        <h2 className="text-base font-semibold text-white mb-2">Verify GuruFocus listing</h2>
        <p className="text-sm text-gray-400 leading-relaxed mb-4">
          Open the URL below to confirm it points to the right company. The
          ticker and exchange combination must match GuruFocus exactly,
          otherwise no price data will be available for this company.
        </p>
        <div className="bg-[#0f1117] border border-gray-800/60 rounded-lg p-3 mb-4 space-y-1.5 text-sm">
          <div className="flex justify-between gap-4">
            <span className="text-gray-500">Name</span>
            <span className="text-gray-200 font-medium text-right">{pendingAdd.company_name}</span>
          </div>
          <div className="flex justify-between gap-4">
            <span className="text-gray-500">Ticker</span>
            <span className="font-mono text-gray-200">{pendingAdd.gurufocus_ticker}</span>
          </div>
          <div className="flex justify-between gap-4">
            <span className="text-gray-500">Exchange</span>
            <span className="font-mono text-gray-200">{pendingAdd.gurufocus_exchange}</span>
          </div>
          <div className="pt-2 mt-2 border-t border-gray-800/60">
            <a
              href={url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-indigo-400 hover:text-indigo-300 hover:underline break-all transition-colors"
            >
              {url}
            </a>
          </div>
        </div>
        <p className="text-sm text-gray-300 mb-4">
          Is this the company you mean to add?
        </p>
        <div className="flex justify-end gap-2">
          <button
            onClick={onCancel}
            disabled={confirming}
            className="px-4 py-2 rounded-lg text-sm font-medium text-gray-400 hover:text-white hover:bg-white/5 transition-colors disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            disabled={confirming}
            className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 inline-flex items-center gap-2"
          >
            {confirming && <Spinner size={14} className="h-3.5 w-3.5 text-white" />}
            {confirming ? 'Adding…' : 'Yes, add this company'}
          </button>
        </div>
      </div>
    </div>
  );
}
