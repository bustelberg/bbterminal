import { describe, expect, it } from 'vitest';

import { bundlePortfolioRefreshJobs, friendlyJobMessage, portfolioJobMessage, unavailableNotice } from './JobToaster';
import { isAirsBusy } from '../../../lib/stores/jobs';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

describe('simple portfolio progress', () => {
  it('does not run stock-price recalculation after refreshing the AIRS overview', () => {
    const source = readFileSync(join('app', 'components', 'PortfolioOverviewPanel.tsx'), 'utf8');
    expect(source).not.toContain('/api/airs/model-portfolios/refresh/job');
  });
  it.each([
    'stored: TOPS_DEF_BEH. composition dated 2026-06-08, scanned 2026-10-30T10:07:54',
    'USD 2000-01-03 → 2026-11-02 extending forward…',
    'US11135F1012 AVGO USD Broadcom Inc.',
    'YTD +2.5123% (contributions sum to +2.5123pp — reconciles)',
  ])('keeps old server diagnostics out of the user message: %s', (message) => {
    expect(portfolioJobMessage({ status: 'running', message }, 'en')).toBe('Updating portfolio data…');
  });

  it('shows the portfolio name and a clear activity', () => {
    const message = 'Bustelberg portfolios · Bustelberg Offensief — Reading from AIRS…';
    expect(portfolioJobMessage({ status: 'running', message }, 'en')).toBe(message);
    expect(portfolioJobMessage({ status: 'running', message: 'Updating stock prices…' }, 'en'))
      .toBe('Updating stock prices…');
  });

  it('reports failure without exposing a traceback or claiming success', () => {
    expect(portfolioJobMessage({ status: 'failed', message: 'RuntimeError: bad price' }, 'en'))
      .toBe('Some data could not be updated. Please try again.');
  });
});

describe('AIRS busy receipts', () => {
  it.each(['refresh', 'scan'])('recognizes a refused %s so it cannot advance the workflow', (action) => {
    expect(isAirsBusy({ status: 'done', summary:
      `Another AIRS ${action} is already running — nothing was re-read` })).toBe(true);
  });

  it('keeps successful refreshes and genuine failures distinct', () => {
    expect(isAirsBusy({ status: 'done', summary: '44/44 accounts, 1200 holdings' })).toBe(false);
    expect(isAirsBusy({ status: 'failed', summary: 'Another AIRS scan is already running — nothing was re-read' })).toBe(false);
    expect(isAirsBusy({ status: 'done', summary: null })).toBe(false);
  });
});

describe('unavailableNotice', () => {
  it('turns the old TSX receipt into a complete user-facing explanation', () => {
    expect(unavailableNotice(
      'Alimentation Couche — unavailable: Alimentation Couche-Tard: '
      + 'TSX is outside the GuruFocus subscription',
    )).toEqual({
      company: 'Alimentation Couche-Tard',
      reason: 'This is a Canadian listing (TSX), which is outside our GuruFocus subscription.',
    });
  });

  it('preserves an already explanatory reason', () => {
    expect(unavailableNotice(
      'Holding — unavailable: Issuer: This market is not supported.',
    )).toEqual({ company: 'Issuer', reason: 'This market is not supported.' });
  });

  it('does not reinterpret ordinary job summaries', () => {
    expect(unavailableNotice('3 companies refetched')).toBeNull();
  });
});

describe('the job cancel control', () => {
  it('uses the reader language instead of a hard-coded English label', () => {
    const source = readFileSync(join('app', 'components', 'jobs', 'JobToaster.tsx'), 'utf8');
    expect(source).toContain("cancel: 'Annuleren'");
    expect(source).toContain('{copy.cancel}');
    expect(source).toContain('{job.cancelRequested && running ? copy.cancelling');
  });
});

describe('friendlyJobMessage', () => {
  it('makes a cancelled portfolio refresh concise and reader-facing', () => {
    expect(friendlyJobMessage('cancelled before Bustelberg Offensief \u2014 nothing was read', 'en'))
      .toBe('Cancelled before Bustelberg Offensief. No changes were made.');
  });

  it('uses Dutch cancellation copy when Dutch is selected', () => {
    expect(friendlyJobMessage('cancelled before Bustelberg Offensief \u2014 nothing was read', 'nl'))
      .toBe('Geannuleerd vóór Bustelberg Offensief. Er zijn geen wijzigingen gemaakt.');
  });
});

describe('friendly AIRS progress', () => {
  it('keeps the group and current portfolio visible after each report', () => {
    expect(friendlyJobMessage('Bustelberg portfolios · Bustelberg 2.0 Offensief · Rendement: ok — stored (0.3s)', 'en'))
      .toBe('Bustelberg portfolios · Bustelberg 2.0 Offensief · The returns have been updated.');
    expect(friendlyJobMessage('Toppenberg portfolios · Toppenberg Offensief · Vermogensoverzicht: failed — timeout', 'nl'))
      .toBe('Toppenberg portfolios · Toppenberg Offensief · De portefeuilleposities konden niet worden bijgewerkt.');
  });

  it('turns a technical report receipt into a plain progress update', () => {
    expect(friendlyJobMessage('Rendement: ok. stored (0.3s)', 'en'))
      .toBe('The returns have been updated.');
    expect(friendlyJobMessage('Vermogensoverzicht: ok. 31 holdings as of 2026-10-16 (1.2s)', 'nl'))
      .toBe('De portefeuilleposities zijn bijgewerkt.');
  });
});

describe('portfolio refresh toast bundle', () => {
  it('keeps the model stage as the single continuation card', () => {
    const jobs = [
      { id: 'accounts', kind: 'airs.vermogen.refresh' },
      { id: 'models', kind: 'airs.models.scan' },
    ] as never[];
    expect(bundlePortfolioRefreshJobs(jobs).map((job) => job.id)).toEqual(['models']);
  });

  it('keeps an account-only refresh visible', () => {
    const jobs = [{ id: 'accounts', kind: 'airs.vermogen.refresh' }] as never[];
    expect(bundlePortfolioRefreshJobs(jobs).map((job) => job.id)).toEqual(['accounts']);
  });
});
