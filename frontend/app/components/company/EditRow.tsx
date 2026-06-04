'use client';

import { useState } from 'react';
import Spinner from '../Spinner';
import { inputCls } from './styles';
import type { Company } from './types';

// ─── Inline edit row ──────────────────────────────────────────────────────────

export default function EditRow({
  company,
  exchangeOptions,
  onSave,
  onCancel,
}: {
  company: Company;
  exchangeOptions: string[];
  onSave: (updated: Partial<Company>) => Promise<void>;
  onCancel: () => void;
}) {
  const [name, setName] = useState(company.company_name ?? '');
  const [ticker, setTicker] = useState(company.gurufocus_ticker);
  const [exchange, setExchange] = useState(company.gurufocus_exchange);
  const [saving, setSaving] = useState(false);

  async function handleSave() {
    setSaving(true);
    await onSave({ company_name: name, gurufocus_ticker: ticker, gurufocus_exchange: exchange });
    setSaving(false);
  }

  return (
    <tr className="border-b border-gray-800/30 bg-indigo-500/5">
      <td className="px-4 py-2 text-sm text-gray-500">{company.company_id}</td>
      <td className="px-3 py-2"><input value={name} onChange={(e) => setName(e.target.value)} className={inputCls} /></td>
      <td className="px-3 py-2"><input value={ticker} onChange={(e) => setTicker(e.target.value)} className={inputCls} /></td>
      <td className="px-3 py-2">
        <input list="edit-exchange" value={exchange} onChange={(e) => setExchange(e.target.value)} className={inputCls} />
        <datalist id="edit-exchange">{exchangeOptions.map((o) => <option key={o} value={o} />)}</datalist>
      </td>
      <td className="px-3 py-2 text-gray-400">{company.country ?? '—'}</td>
      <td className="px-3 py-2 text-gray-600 text-xs">—</td>
      <td className="px-3 py-2">
        <div className="flex gap-1.5">
          <button onClick={handleSave} disabled={saving} className="px-3 py-1.5 rounded-lg text-xs font-medium bg-indigo-600 hover:bg-indigo-500 text-white disabled:opacity-50 transition-colors inline-flex items-center gap-1.5">
            {saving && <Spinner size={12} className="h-3 w-3 text-white" />}
            {saving ? 'Saving…' : 'Save'}
          </button>
          <button onClick={onCancel} className="px-3 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition-colors">
            Cancel
          </button>
        </div>
      </td>
    </tr>
  );
}
