'use client';

/**
 * `usePortfolios` — CRUD for earnings-dashboard portfolios (baskets of
 * companies with weights). Backed by `/api/earnings/portfolios`. Reads work
 * for any authenticated user; create/rename/delete are admin-only server-side
 * (the mutations surface the 403 as an error for non-admins).
 */
import { useCallback, useEffect, useState } from 'react';

import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';

export type PortfolioMember = {
  company_id: number;
  weight: number;
  ticker: string | null;
  name: string | null;
};

export type Portfolio = {
  id: number;
  name: string;
  updated_at: string | null;
  members: PortfolioMember[];
};

export type PortfolioMemberInput = { company_id: number; weight?: number };

async function parse(r: Response): Promise<{ ok: boolean; body: unknown }> {
  const body = await r.json().catch(() => ({}));
  return { ok: r.ok, body };
}

export function usePortfolios() {
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const reload = useCallback(async () => {
    setLoading(true);
    try {
      const r = await apiFetch(`${API_URL}/api/earnings/portfolios`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      setPortfolios((await r.json()) as Portfolio[]);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { reload(); }, [reload]);

  const create = useCallback(async (name: string, members: PortfolioMemberInput[]) => {
    const { ok, body } = await parse(await apiFetch(`${API_URL}/api/earnings/portfolios`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, members }),
    }));
    if (!ok) throw new Error((body as { detail?: string }).detail ?? 'Create failed');
    await reload();
    return body as Portfolio;
  }, [reload]);

  const update = useCallback(async (
    id: number,
    patch: { name?: string; members?: PortfolioMemberInput[] },
  ) => {
    const { ok, body } = await parse(await apiFetch(`${API_URL}/api/earnings/portfolios/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(patch),
    }));
    if (!ok) throw new Error((body as { detail?: string }).detail ?? 'Update failed');
    await reload();
    return body as Portfolio;
  }, [reload]);

  const remove = useCallback(async (id: number) => {
    const { ok, body } = await parse(await apiFetch(`${API_URL}/api/earnings/portfolios/${id}`, {
      method: 'DELETE',
    }));
    if (!ok) throw new Error((body as { detail?: string }).detail ?? 'Delete failed');
    await reload();
  }, [reload]);

  return { portfolios, loading, error, reload, create, update, remove };
}
