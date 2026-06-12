'use client';

/**
 * `useEarningsUniverses` — lists frozen universe snapshots that can be charted
 * as equal-weighted baskets in the earnings Portfolio dropdown. Backed by
 * `GET /api/earnings/universes` (under the earnings auth tier, so any
 * authenticated user can read it). Read-only — these are created on /acwi and
 * /leonteq via the Freeze button, not here.
 */
import { useCallback, useEffect, useState } from 'react';

import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import type { Basket } from './usePortfolios';

export type UniverseBasket = {
  universe_id: number;
  label: string;
  count: number;
};

export function universeToBasket(u: UniverseBasket): Basket {
  return { kind: 'universe', id: u.universe_id, name: u.label, memberCount: u.count };
}

export function useEarningsUniverses() {
  const [universes, setUniverses] = useState<UniverseBasket[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const reload = useCallback(async () => {
    setLoading(true);
    try {
      const r = await apiFetch(`${API_URL}/api/earnings/universes`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      setUniverses((await r.json()) as UniverseBasket[]);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { reload(); }, [reload]);

  return { universes, loading, error, reload };
}
