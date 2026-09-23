import { apiFetch } from './apiFetch';
import { API_URL } from './apiUrl';
import { startLocalJob } from './stores/jobs';

type SectorOverrideOptions = {
  companyId: number;
  companyName: string;
  sector: string | null;
  automaticSector?: string | null;
  afterSave?: () => void | Promise<void>;
};

/**
 * Save a company-wide sector choice through the shared job toaster.
 *
 * A sector write is quick, but the screen re-read it triggers is not. Keeping the whole operation
 * in one root-level toast acknowledges the click immediately and lets the editor close while the
 * affected holdings, composition and attribution views catch up in the background.
 */
export function startSectorOverride(options: SectorOverrideOptions): string {
  const { companyId, companyName, sector, automaticSector, afterSave } = options;
  const target = sector ?? `Automatic${automaticSector ? ` (${automaticSector})` : ''}`;

  return startLocalJob(
    'Updating company sector',
    `company.sector:${companyId}`,
    async (signal, report) => {
      report({ done: 0, total: 2,
        message: `Updating the sector of ${companyName} to ${target} in the background…` });
      const response = await apiFetch(`${API_URL}/api/companies/${companyId}/sector-override`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sector }),
        signal,
      });
      if (!response.ok) {
        const body = (await response.json().catch(() => null)) as { detail?: string } | null;
        throw new Error(body?.detail ?? `Could not save the sector (HTTP ${response.status}).`);
      }
      report({ done: 1, total: 2,
        message: `Saved ${target}; refreshing all affected views…` });
      if (!signal.aborted) await afterSave?.();
      report({ done: 2, total: 2, message: `${companyName} now uses ${target}` });
      return `${companyName} now uses ${target}`;
    },
  );
}
