import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { ExpectedJobFailure, startJob, startLocalJob } from '../../lib/stores/jobs';

/** Source work runs on the server and reports on the existing global popup stack. */
export async function refreshRequest(url: string, init: RequestInit | undefined, title: string, name?: string, nl = false): Promise<Response> {
  const parsed = new URL(url, 'https://portfolio.invalid');
  const body = init?.body ? JSON.parse(String(init.body)) as Record<string, unknown> : {};
  const match = parsed.pathname.match(/\/airs\/model-portfolios\/(\d+)\/composition$/);
  const operation = match ? 'airs' : parsed.pathname.endsWith('/monthly-performance') ? 'monthly'
    : parsed.pathname.endsWith('/recent-transactions') ? 'transactions'
    : parsed.pathname.endsWith('/price-checks') ? 'prices'
    : parsed.pathname.endsWith('/login') ? 'list' : 'composition';
  const { id, done } = await startJob(`${API_URL}/api/leonteq/refresh/job`, title, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ operation, name: body.name ?? name,
      portfolio_id: match ? Number(match[1]) : body.airs_portfolio_id,
      datum: parsed.searchParams.get('datum') || undefined, lang: nl ? 'nl' : 'en',
      source: body.source, isin: body.isin, holding_name: body.holding_name, days: body.days }),
  }).catch((cause) => {
    const message = nl ? 'Vernieuwen kon niet worden gestart. Controleer de verbinding en probeer opnieuw.'
      : 'The refresh could not be started. Check the connection and retry.';
    startLocalJob(title, 'leonteq.refresh.start', async () => { throw new ExpectedJobFailure(message); }, { cancellable: false });
    throw cause;
  });
  const job = await done;
  if (job.status === 'cancelled') throw new ExpectedJobFailure(nl ? 'Vernieuwen gestopt.' : 'Refresh stopped.');
  try {
    const response = await apiFetch(`${API_URL}/api/leonteq/refresh/jobs/${encodeURIComponent(id)}/result`, { cache: 'no-store', noReadCache: true });
    if (!response.ok) throw new ExpectedJobFailure(job.summary ?? (nl ? 'Vernieuwen mislukt. Probeer opnieuw.' : 'Refresh failed. Please retry.'));
    const result = await response.json() as { status: number; payload: unknown };
    return new Response(JSON.stringify(result.payload), { status: result.status, headers: { 'Content-Type': 'application/json' } });
  } catch (cause) {
    if (job.status === 'done') {
      const message = nl ? 'De bron is bijgewerkt, maar de gegevens konden niet worden geladen. Heropen deze weergave.'
        : 'The source was refreshed, but the updated data could not be loaded. Reopen this view.';
      startLocalJob(title, 'leonteq.refresh.result', async () => { throw new ExpectedJobFailure(message); }, { cancellable: false });
      throw new ExpectedJobFailure(message);
    }
    throw cause;
  }
}

/** Quick saved-data reloads use the same popup, without offering a misleading cancel. */
export function reloadWithProgress<T>(title: string, read: () => Promise<T>, message: string, summary: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    startLocalJob(title, 'leonteq.options', async (_signal, report) => {
      report({ done: 0, total: 1, message });
      try {
        const result = await read();
        report({ done: 1, total: 1, message: summary });
        resolve(result);
        return summary;
      } catch (cause) {
        const failure = new ExpectedJobFailure('The saved portfolio options could not be reloaded. Please retry.');
        reject(cause);
        throw failure;
      }
    }, { cancellable: false });
  });
}
