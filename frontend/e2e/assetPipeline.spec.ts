import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';

import { mockAuth } from './_mocks/auth';
import { FILTER_TITLES, mockAssetPipeline } from './_mocks/assetPipeline';

/**
 * /asset-pipeline — the geography contract on the instrument grid.
 *
 * The grid's country/continent/region columns come from two DIFFERENT sources
 * that deliberately disagree (issuer domicile vs listing venue), and the region
 * is MSCI's financial bucket rather than a geographic one. These tests pin the
 * three things that would silently regress: the cross-listing annotation, the
 * faceted narrowing between the three dropdowns, and their required order.
 */

/** The symbol cell is a link — the "row is rendered" signal. */
const symbol = (name: string) => ({ name, exact: true });

/**
 * A filter dropdown by its `title`. Scoped to `select` because the matching
 * COLUMN HEADER carries a longer tooltip that starts with the same words, and
 * `getByTitle` matches on substring.
 */
const filter = (page: Page, title: string) => page.locator(`select[title="${title}"]`);

test.describe('/asset-pipeline geography', () => {
  test.beforeEach(async ({ page }) => {
    await mockAuth(page);
    await mockAssetPipeline(page);
    await page.goto('/asset-pipeline');
    await expect(page.getByRole('link', symbol('AAPL'))).toBeVisible();
  });

  test('the toolbar does not MOVE when you type in the search box', async ({ page }) => {
    /**
     * A <select> sizes itself to its widest <option>, and these options are FACETED — their
     * labels carry live counts ("All sectors (9)" -> "All sectors (1)") and options drop out
     * entirely as the search narrows. So every keystroke used to resize six dropdowns at once,
     * shifting each control sideways and re-flowing the wrapped row under the cursor.
     *
     * The CONTENT of the dropdowns must keep changing — that is the whole point of faceting.
     * Their GEOMETRY must not. This pins the difference.
     */
    const controls = [
      page.getByPlaceholder('Search ISIN / name / symbol / FIGI…'),
      ...Object.values(FILTER_TITLES).map((t) => filter(page, t)),
      page.getByRole('button', { name: 'Clear filters' }),
      page.getByRole('button', { name: '+ Create universe' }),
    ];
    const geometry = async () =>
      Promise.all(controls.map(async (c) => JSON.stringify(await c.boundingBox())));

    const before = await geometry();

    await page.getByPlaceholder('Search ISIN / name / symbol / FIGI…').fill('AAPL');
    await expect(page.getByRole('link', symbol('AAPL'))).toBeVisible();

    // The facets DID narrow — otherwise this test would pass on a broken-but-frozen toolbar.
    await expect(filter(page, FILTER_TITLES.sector)).toContainText('All sectors (1)');
    // ...and not one control moved a pixel.
    expect(await geometry()).toEqual(before);
  });

  test('renders country, continent and region per row', async ({ page }) => {
    const tsm = page.getByRole('row').filter({ has: page.getByRole('link', symbol('TSM')) });
    await expect(tsm).toContainText('Taiwan');
    await expect(tsm).toContainText('Asia');
    await expect(tsm).toContainText('Emerging Markets');

    // Japan is Asia but DEVELOPED — the geographic/financial split.
    const toyota = page.getByRole('row').filter({ has: page.getByRole('link', symbol('7203.T')) });
    await expect(toyota).toContainText('Asia');
    await expect(toyota).toContainText('Pacific');
  });

  test('cross-listed rows annotate the listing venue, same-country rows do not', async ({ page }) => {
    // TSM (Taiwanese issuer, US listing) and ASML (Dutch issuer, US listing).
    const tsm = page.getByRole('row').filter({ has: page.getByRole('link', symbol('TSM')) });
    await expect(tsm).toContainText('via United States');

    const asml = page.getByRole('row').filter({ has: page.getByRole('link', symbol('ASML')) });
    await expect(asml).toContainText('via United States');

    // Apple domiciles where it lists — no annotation.
    const aapl = page.getByRole('row').filter({ has: page.getByRole('link', symbol('AAPL')) });
    await expect(aapl).not.toContainText('via');

    // An ETF has no domicile, so it can never read as cross-listed.
    const spy = page.getByRole('row').filter({ has: page.getByRole('link', symbol('SPY')) });
    await expect(spy).not.toContainText('via');
  });

  test('filters render in the required order: sector, country, continent, region', async ({ page }) => {
    const selects = page.locator('select[title]');
    const titles = await selects.evaluateAll((els) =>
      els.map((e) => (e as HTMLSelectElement).title),
    );
    const geo = titles.filter((t) =>
      ([FILTER_TITLES.sector, FILTER_TITLES.country, FILTER_TITLES.continent, FILTER_TITLES.region] as string[]).includes(t),
    );
    expect(geo).toEqual([
      FILTER_TITLES.sector,
      FILTER_TITLES.country,
      FILTER_TITLES.continent,
      FILTER_TITLES.region,
    ]);
  });

  test('continent filter narrows the visible rows', async ({ page }) => {
    await filter(page, FILTER_TITLES.continent).selectOption('Asia');

    await expect(page.getByRole('link', symbol('TSM'))).toBeVisible();
    await expect(page.getByRole('link', symbol('7203.T'))).toBeVisible();
    await expect(page.getByRole('link', symbol('AAPL'))).toHaveCount(0);
    await expect(page.getByRole('link', symbol('ASML'))).toHaveCount(0);
  });

  test('country filter narrows to one issuer, by DOMICILE not listing', async ({ page }) => {
    // TSM lists in the United States. Filtering on Taiwan must still find it,
    // and must NOT drag in the other US-listed rows.
    await filter(page, FILTER_TITLES.country).selectOption('Taiwan');

    await expect(page.getByRole('link', symbol('TSM'))).toBeVisible();
    await expect(page.getByRole('link', symbol('AAPL'))).toHaveCount(0);
    await expect(page.getByRole('link', symbol('ASML'))).toHaveCount(0);
  });

  test('region filter separates developed Pacific from Emerging Markets', async ({ page }) => {
    await filter(page, FILTER_TITLES.region).selectOption('Emerging Markets');
    await expect(page.getByRole('link', symbol('TSM'))).toBeVisible();
    await expect(page.getByRole('link', symbol('7203.T'))).toHaveCount(0);

    await filter(page, FILTER_TITLES.region).selectOption('Pacific');
    await expect(page.getByRole('link', symbol('7203.T'))).toBeVisible();
    await expect(page.getByRole('link', symbol('TSM'))).toHaveCount(0);
  });

  test('the geography facets narrow each other', async ({ page }) => {
    // Picking a continent must restrict the COUNTRY options to that continent's
    // countries — the faceted-count contract, not just row filtering.
    await filter(page, FILTER_TITLES.continent).selectOption('Asia');

    const countryOpts = await filter(page, FILTER_TITLES.country)
      .locator('option').evaluateAll((els) => els.map((e) => (e as HTMLOptionElement).value));

    expect(countryOpts).toContain('Taiwan');
    expect(countryOpts).toContain('Japan');
    expect(countryOpts).not.toContain('Netherlands');
    expect(countryOpts).not.toContain('United States');
  });

  test('clear filters restores every row', async ({ page }) => {
    await filter(page, FILTER_TITLES.country).selectOption('Taiwan');
    await expect(page.getByRole('link', symbol('AAPL'))).toHaveCount(0);

    await page.getByRole('button', { name: /clear filters/i }).click();

    for (const s of ['AAPL', 'TSM', 'ASML', '7203.T', 'SPY', 'BTC-USD']) {
      await expect(page.getByRole('link', symbol(s))).toBeVisible();
    }
  });
});
