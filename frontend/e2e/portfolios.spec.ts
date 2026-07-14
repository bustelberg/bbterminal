import { expect, test } from '@playwright/test';

import { mockAuth } from './_mocks/auth';
import { mockPortfolios } from './_mocks/portfolios';

/**
 * /portfolios — the AIRS model-portfolio table.
 *
 * What is worth regression-testing here is NOT that a number renders. It is that the three
 * ways a number can be ABSENT stay visibly different from each other and from zero:
 *
 *   n/a   we cannot price enough of the model to say anything (the 60% coverage floor)
 *   —     the model is too young for a ratio (under 20 trading days), or the ratio's
 *         denominator is undefined (a curve that never fell has no downside deviation)
 *   ⚠     the YTD exists but is a BACKTEST of weights chosen with hindsight
 *
 * Collapse any of those into a blank or a 0 and the table starts making claims it cannot
 * support — a 75.78% YTD on a model defined eight days ago is the best-performing portfolio
 * in the list, until you notice it never held those weights.
 */
test.describe('/portfolios', () => {
  test.beforeEach(async ({ page }) => {
    await mockAuth(page);
    await mockPortfolios(page);
    await page.goto('/portfolios');
  });

  const row = (page: import('@playwright/test').Page, name: string) =>
    page.locator('tr').filter({ hasText: name }).first();

  test('the performance columns are all there and sortable', async ({ page }) => {
    for (const label of ['YTD (€)', 'Since incep. (€)', 'Sharpe', 'Sortino']) {
      await expect(page.getByRole('button', { name: label })).toBeVisible();
    }
    await expect(page.getByRole('columnheader', { name: 'Fixed date' })).toBeVisible();
  });

  test('a portfolio with no composition shows an absent number, not a pending one', async ({ page }) => {
    // A `normaal` portfolio has no fixed model, so AIRS stores no composition for it and no
    // performance row will EVER arrive. Wearing the same "…" the priced rows wear while they
    // load would leave it looking like it is still loading, for ever.
    await page.getByRole('checkbox', { name: /Hide small portfolios/ }).uncheck();
    const r = row(page, 'BUS_Neutraal_Dyn');
    await expect(r).not.toContainText('…');
    // Resolved, YTD, Since, Sharpe, Sortino, CAGR, Years — every column needing a composition.
    await expect(r.getByTitle(/nothing to price/)).toHaveCount(7);
  });

  test('a model that predates the year shows a return AND its ratios', async ({ page }) => {
    const r = row(page, 'AITopSelectie OFF FX');
    await expect(r).toContainText('+51.48%');    // YTD — real, the weights were held all year
    await expect(r).toContainText('+50.61%');    // since inception
    await expect(r).toContainText('3.65');       // Sharpe
    await expect(r).toContainText('5.61');       // Sortino
  });

  test('the fixed date shows the model date under it when the two disagree', async ({ page }) => {
    // AIRS's list says 2025-12-29; the composition we hold and measure from is 2025-12-30.
    // Showing only the first, beside three columns anchored on the second, invites the reader
    // to check a return against a window it was never computed over.
    const r = row(page, 'AITopSelectie OFF FX');
    await expect(r).toContainText('2025-12-29');
    await expect(r).toContainText('model 2025-12-30');
  });

  test('an eight-day-old model gets a PARTIAL-year YTD, a ⚠, and NO ratios', async ({ page }) => {
    const r = row(page, 'MoTopSelectie_FX');
    await expect(r).toContainText('⚠');
    // The YTD window opens at the inception, so it is what the model actually made — NOT the
    // +75.78% it would read priced back to a January it never traded in.
    await expect(r).not.toContainText('75.78');
    await expect(r.getByText('+0.51%')).toHaveCount(2);   // YTD and since-inception coincide
    await expect(r.getByTitle(/PARTIAL YEAR — measured from 2026-07-05/)).toBeVisible();
    // One daily return is not a Sharpe. It must not render as 0.00, and must not be blank.
    await expect(r.getByTitle(/Only 1 trading day/)).toHaveCount(2);   // Sharpe + Sortino
  });

  test('Resolved reconciles with Holdings, and is what explains an n/a row', async ({ page }) => {
    // Fully priced: a bare count, no ratio to draw the eye.
    await expect(row(page, 'AITopSelectie OFF FX').getByTitle(/All 24 instrument/)).toBeVisible();

    // TOPS_OFF_BEH holds 10 (9 structured products + cash) and we can price NONE of the nine.
    // The row's four n/a's have a visible cause instead of being an unexplained blank.
    const thin = row(page, 'TOPS_OFF_BEH');
    await expect(thin).toContainText('0/9');
    await expect(thin.getByTitle(/0 of 9 instrument\(s\) have a Yahoo price series/)).toBeVisible();
  });

  test('an unpriceable model refuses every number, and says why', async ({ page }) => {
    const r = row(page, 'TOPS_OFF_BEH');
    // YTD, Since, Sharpe, Sortino, CAGR — every derived figure is refused for the SAME reason:
    // there is no honest return underneath them to divide or annualize.
    await expect(r.getByText('n/a')).toHaveCount(5);
    await expect(r.getByTitle(/Only 1% of this model's weight can be priced/)).not.toHaveCount(0);
    await expect(r).not.toContainText('%');                   // never a renormalised invention
  });

  test('hide small portfolios keeps ONLY the countable models over 5 holdings', async ({ page }) => {
    // A KEEP rule, not a drop rule: checked, the table shows the real models and nothing else.
    const small = page.getByText('TOPS_MTS_L');          // a counted model holding 1 instrument
    const noModel = page.getByText('BUS_Neutraal_Dyn');  // no composition at all
    const real = page.getByText('AITopSelectie OFF FX'); // 24 holdings

    const box = page.getByRole('checkbox', { name: /Hide small portfolios/ });
    await expect(box).toBeChecked();                     // on by default
    await expect(real).toBeVisible();
    await expect(small).toHaveCount(0);
    await expect(noModel).toHaveCount(0);                // no countable model -> also hidden

    await box.uncheck();                                 // ...and unchecking brings both back
    await expect(small).toBeVisible();
    await expect(noModel).toBeVisible();
  });

  test('expanding a portfolio shows the price marks behind its YTD', async ({ page }) => {
    await page.getByText('AITopSelectie OFF FX').click();

    // The window the marks are measured from — "since when" is half of what a return means.
    await expect(page.getByText('marks from')).toBeVisible();

    // The expanded block is itself a <tr>, so every position row matches twice — take the inner.
    const posRow = (t: string) => page.locator('tr').filter({ hasText: t }).last();

    const amazon = posRow('US0231351067');
    await expect(amazon).toContainText('196.44');      // EUR entry mark
    await expect(amazon).toContainText('2025-12-31');  // ...and the close it came from
    await expect(amazon).toContainText('212.89');      // EUR exit mark
    await expect(amazon).toContainText('2026-07-02');
    await expect(amazon).toContainText('+8.37%');      // the EUR return between them

    // A holding whose close LAGS the others is marked at its last known price, and the date
    // says so — it is not an error and it is not a gap.
    await expect(posRow('US0378331005')).toContainText('2026-06-30');

    // An unresolved ETF has NO marks — and a 0% return would be a lie, not a blank. (Its WEIGHT
    // is still 5.00%: the model holds it, we just cannot price it. That is the whole point.)
    const etf = posRow('IE00077FRP95');
    await expect(etf).toContainText('5.00%');
    await expect(etf.locator('td').last()).toHaveText('—');    // the Return cell
    await expect(etf.getByTitle(/not an instrument in our grid/).first()).toBeVisible();

    // Cash is priced at a flat 0% INSIDE the portfolio return; it has no series of its own.
    await expect(posRow('Liquiditeiten').getByTitle(/priced at a flat 0%/).first()).toBeVisible();
  });

  test('an interpolated opening price is marked as an estimate, not shown as a close', async ({ page }) => {
    await page.getByText('AITopSelectie OFF FX').click();
    const bond = page.locator('tr').filter({ hasText: 'IE00B66F4759' }).last();

    // The price renders in the same column and font as an observed one, so it has to SAY it is
    // not one — otherwise a modelled number is indistinguishable from a traded price.
    await expect(bond).toContainText('92.63');
    await expect(bond).toContainText('(est)');
    await expect(bond).toContainText('⚠');
    await expect(bond.getByTitle(/ESTIMATE, not a traded price.*127 days apart/).first())
      .toBeVisible();
  });

  test('a CAGR is REFUSED under a year, and shown over one', async ({ page }) => {
    // Measured, not extrapolated: 1.27 years of evidence behind the rate.
    const mature = row(page, 'BUS_Risicodragend');
    await expect(mature).toContainText('+35.97%');
    await expect(mature).toContainText('1.27');

    // 0.54 years. Annualizing its +50.61% would print +114.8% beside that +35.97% — same column,
    // same font, a third of the evidence. So there is no number, and the Years cell says why.
    const young = row(page, 'AITopSelectie OFF FX');
    await expect(young).toContainText('0.54');
    await expect(young.getByTitle(/under one\. Annualizing a shorter window extrapolates it/))
      .toBeVisible();
    await expect(young).not.toContainText('114.8');
  });

  test('Analyse opens the composition, and does NOT also expand the row', async ({ page }) => {
    const r = row(page, 'AITopSelectie OFF FX');
    await r.getByRole('button', { name: 'Analyse' }).click();

    const modal = page.getByRole('dialog');
    await expect(modal).toBeVisible();
    await expect(modal).toContainText('Composition vs');
    // Two series -> a legend is mandatory; identity is never colour-alone. One legend per CHART
    // (scoped to the legend text, since the returns tile also has a "Portfolio" column header).
    const legend = modal.locator('.recharts-legend-item-text');
    await expect(legend.filter({ hasText: /^Portfolio$/ })).toHaveCount(3);
    await expect(legend.filter({ hasText: /^SP500$/ })).toHaveCount(3);
    // All three axes.
    for (const axis of ['Sector', 'Region', 'Currency']) {
      await expect(modal.getByRole('heading', { name: axis })).toBeVisible();
    }
    // ⚠ The honest bucket: we do not look through funds, and the chart says so. (The axis label
    // wraps across two SVG tspans, so match the un-broken half.)
    await expect(modal).toContainText('Fund (not looked');
    await expect(modal).toContainText('we hold no look-through');

    // The returns tile — portfolio vs index, over the model's OWN windows, each stating its
    // start date. A benchmark measured over a different period is not a benchmark.
    await expect(modal.getByRole('heading', { name: 'Return (€)' })).toBeVisible();
    await expect(modal).toContainText('+51.48%');    // portfolio YTD
    await expect(modal).toContainText('+12.41%');    // ...and the index over THAT window
    await expect(modal).toContainText('+39.07%');    // excess
    await expect(modal).toContainText('from 2026-01-01');
    await expect(modal).toContainText('from 2025-12-30');   // inception ≠ 1 Jan

    // The button must NOT have toggled the row open — that would be two actions on one press.
    await expect(page.getByText('marks from')).toHaveCount(0);
  });

  test('switching to ACWI states how much of the index we could rebuild', async ({ page }) => {
    await row(page, 'AITopSelectie OFF FX').getByRole('button', { name: 'Analyse' }).click();
    const modal = page.getByRole('dialog');

    // The S&P rebuilds almost completely (488/493) — no warning worth interrupting for.
    await expect(modal.getByText(/This index is rebuilt from/)).toHaveCount(0);

    await modal.getByRole('combobox').selectOption('ACWI');

    // ⚠ ACWI does NOT. Its missing constituents are a whole country at a time (GuruFocus sells
    // no UK or India; some yfinance ISINs were never ingested), and a cap-weighted index
    // renormalised over the rest does not lose that weight — it redistributes it. Say so.
    await expect(modal.getByText(/This index is rebuilt from/)).toBeVisible();
    await expect(modal).toContainText('1346');
    await expect(modal).toContainText('1998');
    await expect(modal).toContainText('67%');
  });

  test('clicking a return row explains WHY — allocation vs selection', async ({ page }) => {
    await row(page, 'AITopSelectie OFF FX').getByRole('button', { name: 'Analyse' }).click();
    const modal = page.getByRole('dialog');

    // The attribution is not shown until asked for — an excess is a fact, the "why" is a question.
    await expect(modal.getByRole('heading', { name: /^Why —/ })).toHaveCount(0);
    await modal.getByText('Since inception').click();

    const panel = modal.getByRole('heading', { name: /^Why —/ });
    await expect(panel).toBeVisible();

    // Brinson separates the two mistakes: the SECTORS you chose vs the STOCKS inside them.
    await expect(modal).toContainText('Allocation');
    await expect(modal).toContainText('Selection');
    await expect(modal).toContainText('-7.51%');   // Industrials: bad SELECTION
    await expect(modal).toContainText('-5.07%');   // Consumer Cyclical: bad ALLOCATION

    // ⚠ The unpriced Healthcare position makes its sector read as UNOWNED, so that row's
    // allocation effect is a FALSE finding — not a missing one. It must be called out.
    await expect(modal.getByText(/we cannot price/)).toBeVisible();
    await expect(modal).toContainText('Healthcare');

    // The other half of "why": the index's winners you did NOT own. Alphabet must NOT be here —
    // the model holds class C, the index holds class A. One company, two ISINs.
    await expect(modal).toContainText(/winners you didn.t own/);
    await expect(modal).toContainText('Apple Inc');
    const missed = modal.locator('table').last();
    await expect(missed).not.toContainText('Alphabet');
  });

  test('the allocation column explains ITSELF — the number alone means nothing', async ({ page }) => {
    await row(page, 'AITopSelectie OFF FX').getByRole('button', { name: 'Analyse' }).click();
    const modal = page.getByRole('dialog');
    await modal.getByText('Since inception').click();
    await expect(modal.getByRole('heading', { name: /^Why —/ })).toBeVisible();

    // The headline conclusion, in a sentence — the reader should not have to subtract two
    // numbers to learn which of the two mistakes this was.
    await expect(modal).toContainText('Mostly the companies, not the sectors');
    await expect(modal).toContainText('cost you 11.52pp');
    await expect(modal).not.toContainText('cost you +');    // the verb carries the sign, not the number

    // The reference point allocation is scored against MUST be on screen: an over/underweight is
    // judged by whether its bucket beat or lagged the INDEX AS A WHOLE, not by whether it rose.
    await expect(modal).toContainText('index total +36.9%');

    // And every cell says, in words, what it means — on HOVER, immediately. (Not the native
    // `title=` attribute: the browser sits on that for 1-2 seconds, by which time the reader has
    // already explained the column to themselves, wrongly.)
    //
    // Consumer Cyclical: a big overweight in a sector that badly lagged the index — the classic
    // misread ("but it went UP").
    const cc = modal.locator('tr').filter({ hasText: 'Consumer Cyclical' });
    await cc.getByText('-5.07%').hover();           // its Allocation value
    await expect(modal.getByText(/34\.0% vs the index's 13\.0%/)).toBeVisible();
    await expect(modal.getByText(/lagged the index/).first()).toBeVisible();
  });

  test('every column header explains itself', async ({ page }) => {
    await row(page, 'AITopSelectie OFF FX').getByRole('button', { name: 'Analyse' }).click();
    const modal = page.getByRole('dialog');
    await modal.getByText('Since inception').click();
    await expect(modal.getByRole('heading', { name: /^Why —/ })).toBeVisible();

    // Every column is a term of art or a subscripted symbol. A reader who has to GUESS what one
    // means will guess wrong in a way that looks right — so each one carries its own explanation,
    // shown on hover IMMEDIATELY. The native `title=` attribute cannot do this: the browser
    // delays it by 1-2 seconds and the delay is not configurable.
    // (Scoped to the attribution section — the Return tile has a thead of its own.)
    const head = modal.locator('section').filter({ hasText: 'Why —' }).locator('thead').first();

    // The one the whole panel exists to prevent: a bucket can rise and still have been the wrong
    // place to be, if it rose by less than the index.
    await head.getByText('Allocation').hover();
    await expect(modal.getByText(/it went up, and it was still the wrong place to be/i))
      .toBeVisible();

    await head.getByText('Selection').hover();
    await expect(modal.getByText(/purely about the picks/)).toBeVisible();

    await head.getByText('Interact.').hover();
    await expect(modal.getByText(/assigned cleanly to either column/).first()).toBeVisible();

    // The subtitles carry the plain-English version, so the meaning survives without a hover.
    await expect(modal).toContainText('right sectors?');
    await expect(modal).toContainText('right companies?');
    await expect(modal).toContainText('index total +36.9%');
  });

  test('the wording follows the axis — never “sector” while showing regions', async ({ page }) => {
    await row(page, 'AITopSelectie OFF FX').getByRole('button', { name: 'Analyse' }).click();
    const modal = page.getByRole('dialog');
    await modal.getByText('Since inception').click();
    const panel = modal.locator('section').filter({ hasText: 'Why —' }).first();
    await expect(panel).toBeVisible();

    // On the sector axis it says sector, and it says "companies" — not the jargon "bucket", and
    // not the vague "names".
    await expect(panel).toContainText('right sectors?');
    await expect(panel).toContainText('right companies?');
    await expect(panel).not.toContainText('bucket');
    await expect(panel.getByText(/Mostly the companies, not the sectors/)).toBeVisible();

    // ⚠ Switch to Region and EVERY "sector" must become "region". Hardcoding the word would make
    // the whole panel lie on two of its three axes — and the header and the legend must agree,
    // because they are describing the same numbers.
    await panel.getByRole('combobox').selectOption('region');
    await expect(panel.getByText('right regions?').first()).toBeVisible();
    await expect(panel).toContainText('region’s share of the excess');
    await expect(panel.getByText(/Mostly the companies, not the regions/)).toBeVisible();
    await expect(panel).not.toContainText('right sectors?');
  });

  test('the formulas are real MathML, not letters glued together', async ({ page }) => {
    await row(page, 'AITopSelectie OFF FX').getByRole('button', { name: 'Analyse' }).click();
    const modal = page.getByRole('dialog');
    await modal.getByText('Since inception').click();
    await expect(modal.getByRole('heading', { name: /^Why —/ })).toBeVisible();

    // Native MathML — no library, no bundle. It buys three things hand-rolled <sub> cannot:
    // italic variables (how maths marks a variable, not a label), real operator spacing, and a
    // TRUE overbar on R̄B rather than a combining macron that lands differently in every font.
    const legend = modal.locator('dl');
    await expect(legend.locator('math')).not.toHaveCount(0);
    await expect(legend.locator('math mi').first()).toBeVisible();   // a variable
    await expect(legend.locator('math msub').first()).toBeVisible();  // its subscript
    await expect(legend.locator('math mover').first()).toBeVisible(); // the overbar on R̄B

    // And the four effects are named, in the same order as the table's columns.
    for (const name of ['Allocation', 'Selection', 'Interaction', 'Total']) {
      await expect(legend.getByText(name, { exact: true })).toBeVisible();
    }
  });

  test('sorting by Sharpe sinks the absent rows in BOTH directions', async ({ page }) => {
    // 2nd cell — the 1st is now the Analyse button.
    const names = async () => (await page.locator('tbody tr td:nth-child(2)').allInnerTexts())
      .map((t) => t.replace(/[▸▾]/g, '').trim());

    const sharpe = page.getByRole('button', { name: /Sharpe/ });
    await sharpe.click();                                     // ascending
    const asc = await names();
    await sharpe.click();                                     // descending
    const desc = await names();

    const withRatio = ['AITopSelectie OFF FX', 'BUS_Risicodragend'];   // 3.65 and 1.98
    // Descending: the highest Sharpe leads.
    expect(desc[0]).toContain('AITopSelectie OFF FX');
    // Ascending: the LOWEST real Sharpe leads — the ratio-less rows do not sort as if they were
    // zero. Absent is not a small number, and it sinks in BOTH directions.
    expect(asc[0]).toContain('BUS_Risicodragend');
    expect(asc.slice(0, 2).join()).toContain(withRatio[0]);
    expect(asc.at(-1)).not.toContain('AITopSelectie OFF FX');
  });

  test('a holding that IS a portfolio gets a Link, and the cycle is not offered', async ({ page }) => {
    // Some positions are not instruments — they are other models, wrapped as a Leonteq
    // certificate. "Star Selection Index" IS StarTopSelectie OFF FX, and can never be priced
    // directly, so the link is the only way to see through it.
    await page.getByText('AITopSelectie OFF FX').click();
    const row = page.locator('tr').filter({ hasText: 'Star Selection Index' }).last();
    await expect(row).toBeVisible();

    const select = row.locator('select');
    await expect(select).toHaveValue('2094');                       // the guess is pre-filled
    await expect(row).toContainText('99%');                         // ...and says how sure it is

    // ⚠ THE CYCLE. TOPS_STS_L is the CLOSEST name match in the whole list — its description is
    // literally "StarTopSelectie" — and it is the one answer that is definitely wrong: it HOLDS
    // this certificate at 100%. Offering it would be offering a loop.
    const labels = await select.locator('option').allInnerTexts();
    expect(labels.join('|')).toContain('StarTopSelectie OFF FX');
    expect(labels.join('|')).not.toContain('TOPS_STS_L');

    // A plain instrument is not linked to anything, and its dropdown says so rather than
    // guessing — "not a portfolio" is an answer, and most rows are exactly that.
    const amazon = page.locator('tr', { hasText: 'Amazon' }).last();
    await expect(amazon.locator('select')).toHaveValue('');
  });
});
