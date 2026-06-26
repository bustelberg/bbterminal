'use client';

import { useCallback, useEffect, useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { apiFetch } from '../../../lib/apiFetch';
import { trackedFetch } from '../../../lib/loading';
import type { PortfolioStateResponse, SavedPortfolio } from '../../../lib/types/api';

/** Live-tracked diversified portfolios (scheduled-strategy base), for the
 * /schedule "Diversified portfolios" lane. State is fetched on demand. */
export function useDiversifiedPortfolios() {
  const [portfolios, setPortfolios] = useState<SavedPortfolio[]>([]);
  const [state, setState] = useState<PortfolioStateResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const res = await apiFetch(`${API_URL}/api/momentum/diversifier/portfolios?scheduled=true`);
      if (res.ok) setPortfolios(await res.json());
    } catch { /* non-fatal */ }
  }, []);

  useEffect(() => { load(); }, [load]);

  const viewState = useCallback(async (id: number) => {
    setError(null);
    try {
      const res = await trackedFetch('Loading portfolio state', `${API_URL}/api/momentum/diversifier/portfolios/${id}/state`);
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(typeof data.detail === 'string' ? data.detail : `HTTP ${res.status}`);
      }
      setState(await res.json());
    } catch (e) {
      setError(`Couldn't load state: ${e instanceof Error ? e.message : e}`);
    }
  }, []);

  const remove = useCallback(async (id: number) => {
    setError(null);
    try {
      const res = await trackedFetch('Deleting portfolio', `${API_URL}/api/momentum/diversifier/portfolios/${id}`, { method: 'DELETE' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setPortfolios((prev) => prev.filter((p) => p.id !== id));
      setState((prev) => (prev?.id === id ? null : prev));
    } catch (e) {
      setError(`Delete failed: ${e instanceof Error ? e.message : e}`);
    }
  }, []);

  return { portfolios, state, error, viewState, remove };
}
