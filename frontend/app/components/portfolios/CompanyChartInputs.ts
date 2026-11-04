import { createContext, useContext, useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';

type Input = { data?: unknown; error?: string };
export type CompanyChartInputs = Record<'own' | 'benchmark', Record<string, Input>>;

export const CompanyChartInputsContext = createContext<CompanyChartInputs | null>(null);

export function useCompanyChartInput<T>(path: string, side: 'own' | 'benchmark' = 'own', enabled = true) {
  const inputs = useContext(CompanyChartInputsContext);
  if (!inputs || !enabled) return null;
  const input = inputs[side][path];
  return { data: (input?.data as T | undefined) ?? null, error: input?.error ?? null };
}

/** Prefer the company bundle. Aggregate views still read their own chart inputs. */
export function useChartHoldingsInputs<T>(path: string, target: object) {
  const bundled = useCompanyChartInput<T>(path);
  const hasBundle = bundled !== null;
  const [fetched, setFetched] = useState<{ data: T | null; error: string | null }>({ data: null, error: null });

  useEffect(() => {
    if (hasBundle) return;
    let active = true;
    setFetched({ data: null, error: null });
    void (async () => {
      try {
        const response = await apiFetch(`${API_URL}/api/earnings/${path}`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(target),
        });
        const body = await response.json().catch(() => null);
        if (!response.ok) throw new Error(body?.detail ?? `HTTP ${response.status}`);
        if (active) setFetched({ data: body as T, error: null });
      } catch (error) {
        if (active) setFetched({ data: null, error: error instanceof Error ? error.message : String(error) });
      }
    })();
    return () => { active = false; };
  }, [hasBundle, path, target]);

  return bundled ?? fetched;
}
