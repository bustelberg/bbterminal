/**
 * `useCompanies` — data fetching, server mutations, and data-derived
 * options for the `/companies` manager.
 *
 * Lifted out of `CompanyManager.tsx` so the god-component stops owning the
 * two-stage load (companies, then the slower memberships merge), the
 * Add/Edit/Delete mutation flows (incl. the 409-duplicate confirm + the
 * GuruFocus exchange-search probe), and the option/duplicate derivations.
 * Pure view state (search / filters / sort) lives in `useCompanyFilters`;
 * `adding` / `editingId` UI toggles stay in the orchestrator.
 */
import { useCallback, useEffect, useMemo, useState } from 'react';

import { apiFetch } from '../../../lib/apiFetch';
import { dialog } from '../../../lib/dialog';
import { trackedFetch } from '../../../lib/loading';
import { API_URL } from '../../../lib/apiUrl';
import type { Company, PendingAdd } from './types';

export type UseCompaniesResult = ReturnType<typeof useCompanies>;

export function useCompanies() {
  const [companies, setCompanies] = useState<Company[]>([]);
  const [loading, setLoading] = useState(true);
  // Universe memberships are fetched as a second, slower roundtrip after the
  // companies list lands. While this is true, the Memberships column shows a
  // small spinner instead of "—" so an empty chip cell isn't mistaken for
  // "this company belongs to no universes".
  const [membershipsLoading, setMembershipsLoading] = useState(true);
  // Sectors are fetched as a third roundtrip (alongside memberships) after the
  // base list. Same treatment: a spinner instead of "—" while in flight so an
  // empty cell isn't mistaken for "this company has no sector".
  const [sectorsLoading, setSectorsLoading] = useState(true);
  // company_id whose Delete request is currently in flight, or null.
  const [deletingId, setDeletingId] = useState<number | null>(null);
  const [pendingAdd, setPendingAdd] = useState<PendingAdd | null>(null);
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setMembershipsLoading(true);
    setSectorsLoading(true);
    try {
      const res = await trackedFetch('Loading companies', `${API_URL}/api/companies`);
      const data: Company[] = await res.json();
      // Companies render immediately with empty memberships. The slower
      // membership aggregate fires in parallel and merges in when ready.
      setCompanies(data.map((c) => ({ ...c, universes: c.universes ?? [] })));
    } catch {
      setError('Failed to load companies');
    }
    setLoading(false);

    // Memberships + sectors are both per-company aggregates fetched after the
    // base list. Run them in parallel and merge each in when it lands.
    const membershipsP = (async () => {
      try {
        const res = await trackedFetch(
          'Loading universe memberships',
          `${API_URL}/api/companies/memberships`,
        );
        const { memberships } = (await res.json()) as { memberships: Record<string, string[]> };
        setCompanies((prev) =>
          prev.map((c) => ({ ...c, universes: memberships[String(c.company_id)] ?? [] })),
        );
      } catch {
        // Non-fatal — chips just don't render.
      } finally {
        setMembershipsLoading(false);
      }
    })();

    const sectorsP = (async () => {
      try {
        const res = await trackedFetch(
          'Loading sectors',
          `${API_URL}/api/companies/sectors`,
        );
        const { sectors } = (await res.json()) as { sectors: Record<string, string> };
        setCompanies((prev) =>
          prev.map((c) => ({ ...c, sector: sectors[String(c.company_id)] ?? null })),
        );
      } catch {
        // Non-fatal — sector column just shows "—".
      } finally {
        setSectorsLoading(false);
      }
    })();

    await Promise.all([membershipsP, sectorsP]);
  }, []);

  useEffect(() => { load(); }, [load]);

  const exchangeOptions = useMemo(() => {
    const s = new Set(companies.map((c) => c.gurufocus_exchange).filter(Boolean));
    return [...s].sort();
  }, [companies]);

  const countryOptions = useMemo(() => {
    const s = new Set(companies.map((c) => c.country).filter((v): v is string => !!v?.trim()));
    return [...s].sort();
  }, [companies]);

  const sectorOptions = useMemo(() => {
    const s = new Set(companies.map((c) => c.sector).filter((v): v is string => !!v?.trim()));
    return [...s].sort();
  }, [companies]);

  const universeOptions = useMemo(() => {
    const s = new Set<string>();
    for (const c of companies) for (const u of c.universes ?? []) s.add(u);
    return [...s].sort();
  }, [companies]);

  // Detect duplicate company names (case-insensitive)
  const duplicateNames = useMemo(() => {
    const counts = new Map<string, number>();
    for (const c of companies) {
      const name = (c.company_name ?? '').toLowerCase().trim();
      if (name) counts.set(name, (counts.get(name) ?? 0) + 1);
    }
    const dupes = new Set<string>();
    for (const [name, count] of counts) {
      if (count > 1) dupes.add(name);
    }
    return dupes;
  }, [companies]);

  // Count of companies that share a name with at least one other company.
  // Click the badge in the header to filter the table to just these rows.
  const duplicateCount = useMemo(() => {
    const nameCounts = new Map<string, number>();
    for (const c of companies) {
      const name = (c.company_name ?? '').trim().toLowerCase();
      if (name) nameCounts.set(name, (nameCounts.get(name) ?? 0) + 1);
    }
    let n = 0;
    for (const c of companies) {
      const name = (c.company_name ?? '').trim().toLowerCase();
      if (name && (nameCounts.get(name) ?? 0) > 1) n++;
    }
    return n;
  }, [companies]);

  const handleSave = useCallback(async (id: number, updated: Partial<Company>) => {
    setError(null);
    try {
      const res = await apiFetch(`${API_URL}/api/companies/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updated),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail ?? `HTTP ${res.status}`);
      }
      const saved = await res.json();
      setCompanies((prev) => prev.map((c) => (c.company_id === id ? { ...c, ...saved } : c)));
      return true;
    } catch (e) {
      setError(`Save failed: ${e instanceof Error ? e.message : e}`);
      return false;
    }
  }, []);

  const handleAdd = useCallback(async (data: PendingAdd) => {
    setError(null);
    setPendingAdd(data);
  }, []);

  const confirmAdd = useCallback(async () => {
    if (!pendingAdd) return;
    setConfirming(true);
    setError(null);
    try {
      // First try without `force`. The backend's canonical dupe check
      // returns 409 with the matching rows; on 409 we surface the
      // matches through a confirm dialog and retry with `force=true`.
      let res = await apiFetch(`${API_URL}/api/companies`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(pendingAdd),
      });
      if (res.status === 409) {
        const body = await res.json().catch(() => ({}));
        type Match = { company_id: number; company_name: string | null; gurufocus_ticker: string; gurufocus_exchange: string | null };
        const matches: Match[] = (body.detail?.matches ?? []) as Match[];
        const lines = matches.map((m) => `  cid=${m.company_id} ${m.gurufocus_exchange ?? '?'}:${m.gurufocus_ticker}  ${m.company_name ?? ''}`).join('\n');
        const proceed = await dialog.confirm(
          `${matches.length} possible duplicate${matches.length === 1 ? '' : 's'} found:\n\n${lines}\n\nAdd as a new company anyway?`,
          { destructive: true, confirmLabel: 'Add anyway' },
        );
        if (!proceed) {
          setPendingAdd(null);
          return;
        }
        res = await apiFetch(`${API_URL}/api/companies`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...pendingAdd, force: true }),
        });
      }
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail ?? `HTTP ${res.status}`);
      }
      setPendingAdd(null);
      await load();
      return true;
    } catch (e) {
      setError(`Add failed: ${e instanceof Error ? e.message : e}`);
      return false;
    } finally {
      setConfirming(false);
    }
  }, [pendingAdd, load]);

  /** Probe GuruFocus across a list of candidate exchanges to find which
   * one actually resolves for this company's ticker. Surfaces the result
   * via `dialog` so the user sees a clear "FOUND on NASDAQ" / "NOT FOUND"
   * message + a one-click 'Update exchange to X' confirmation. The
   * update writes through the same PUT /api/companies/{id} the inline
   * edit uses, so the row refreshes naturally. */
  const findCorrectExchange = useCallback(async (c: Company) => {
    try {
      const res = await apiFetch(`${API_URL}/api/admin/gurufocus-exchange-search`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tickers: [{ ticker: c.gurufocus_ticker, current_exchange: c.gurufocus_exchange }],
        }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        await dialog.alert(`Lookup failed: ${body.detail ?? `HTTP ${res.status}`}`);
        return;
      }
      const data = (await res.json()) as Array<{
        ticker: string;
        current_exchange: string | null;
        found_exchange: string | null;
        status: 'found' | 'not_found';
        candidates_tried: { exchange: string; status_code: number | null; ok: boolean }[];
        error: string | null;
      }>;
      const r = data[0];
      if (!r) {
        await dialog.alert('Lookup returned no result.');
        return;
      }
      const tried = r.candidates_tried.map((t) => `${t.exchange} ${t.ok ? 'OK' : `(${t.status_code ?? 'err'})`}`).join(', ');
      if (r.status === 'found' && r.found_exchange && r.found_exchange !== c.gurufocus_exchange) {
        const ok = await dialog.confirm(
          `${r.ticker} resolved on ${r.found_exchange} (current: ${c.gurufocus_exchange}).\n\nUpdate the row's exchange to ${r.found_exchange}?\n\nTried: ${tried}`,
          { confirmLabel: `Set to ${r.found_exchange}` },
        );
        if (!ok) return;
        // Reuse the inline-edit save path so validation + reload behave
        // identically. The handleSave signature takes a `Partial<Company>`.
        await handleSave(c.company_id, { gurufocus_exchange: r.found_exchange });
      } else if (r.status === 'found') {
        await dialog.alert(`${r.ticker} resolved on ${r.found_exchange} — already the row's exchange. The lookup-failed flag should clear on the next ingest tick.`);
      } else {
        await dialog.alert(`Could not find ${r.ticker} on any candidate exchange.\n\nTried: ${tried}\n\nMight be a stale ticker symbol or genuinely not in GuruFocus.`);
      }
    } catch (e) {
      await dialog.alert(`Lookup failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  }, [handleSave]);

  const handleDelete = useCallback(async (id: number, name: string) => {
    if (!(await dialog.confirm(`Delete "${name}"? This cannot be undone.`, { destructive: true, confirmLabel: 'Delete' }))) return;
    setError(null);
    setDeletingId(id);
    try {
      const res = await apiFetch(`${API_URL}/api/companies/${id}`, { method: 'DELETE' });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail ?? `HTTP ${res.status}`);
      }
      setCompanies((prev) => prev.filter((c) => c.company_id !== id));
    } catch (e) {
      setError(`Delete failed: ${e instanceof Error ? e.message : e}`);
    } finally {
      setDeletingId(null);
    }
  }, []);

  return {
    companies,
    loading,
    membershipsLoading,
    sectorsLoading,
    deletingId,
    error,
    setError,
    load,
    // derived
    exchangeOptions,
    countryOptions,
    sectorOptions,
    universeOptions,
    duplicateNames,
    duplicateCount,
    // mutations
    handleSave,
    handleAdd,
    handleDelete,
    findCorrectExchange,
    // add-confirm flow
    pendingAdd,
    setPendingAdd,
    confirming,
    confirmAdd,
  };
}
