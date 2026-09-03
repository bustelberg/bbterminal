import type { Lang } from '../../../lib/i18n';

/**
 * The Long Equity cards' HEADINGS, in both languages.
 *
 * ⚠⚠ THE HEADING ONLY — not the stat tiles, legends, tooltips or footnotes on these cards. That is
 * the scope that was asked for, and it is a coherent one: a reader scanning fourteen cards in a
 * grid is reading the titles, and the titles are what say which metric they are looking at.
 *
 * ⚠⚠ AND IT IS A SEPARATE LOOKUP FROM `MetricCfg.title`, DELIBERATELY. The four growth cards carry
 * their title inside a config object that is ALSO used as a React `key` in `LongEquityTab`
 * (`key={revenue.title}`), as a series label, and as the fallback display name when a book has
 * none. Translating the config field itself would change the key with the language — remounting
 * every card and refetching its data on a switch that should only repaint text. So the config keeps
 * its English identity and gains a stable `titleKey`; only the rendered `<h4>` is translated.
 *
 * ⚠ EVERY ENTRY IS A FUNCTION OF `sbc`, INCLUDING THE ONES THAT IGNORE IT. Three headings genuinely
 * change with the SBC-correction checkbox (`FCF margin` → `FCF-SBC margin`), and a record mixing
 * bare strings with functions means every call site has to know which kind it is holding. One shape
 * costs a `()` and removes that question.
 *
 * ⚠ THE DUTCH IS LONGER THAN THE ENGLISH, sometimes by a lot — "SBC / OCF" against
 * "Aandelenbeloning / operationele kasstroom". These headings sit in a grid where the cards' stat
 * tiles are read as a row, so a heading that wraps to two lines pushes one card's tiles out of line
 * with its neighbours' (see the ⚠ on `CashReturnCard`'s `<h4>`). That is a real cost of spelling
 * the terms out, accepted because a reader who cannot tell which metric a card shows has a worse
 * problem than a ragged grid.
 */

export const CHART_KEYS = [
  'sharePrice', 'epsNri', 'revenue', 'fcfPs', 'shares',
  'fcfMargin', 'croic', 'roic', 'debtAssets', 'interestBurden', 'sbcOcf',
  'investedCapital', 'capexMargin', 'dividendYield', 'fcfYield', 'grossMargin', 'cashConversion',
] as const;
export type ChartKey = (typeof CHART_KEYS)[number];

/** `(sbcCorrection) => heading`. Most entries ignore the argument — see the ⚠ above. */
export type ChartTitles = Record<ChartKey, (sbc: boolean) => string>;

const en: ChartTitles = {
  sharePrice: () => 'Share price',
  epsNri: () => 'EPS (excl. non-recurring)',
  revenue: () => 'Revenue',
  fcfPs: () => 'FCF per share',
  shares: () => 'Shares outstanding',
  fcfMargin: (sbc) => `${sbc ? 'FCF-SBC' : 'FCF'} margin`,
  croic: () => 'Cash return on capital',
  roic: () => 'Return on invested capital',
  debtAssets: () => 'Debt / assets ex-GW',
  interestBurden: () => 'Interest / op. profit',
  sbcOcf: () => 'SBC / OCF',
  investedCapital: () => 'Invested capital',
  capexMargin: () => 'Capex margin',
  dividendYield: () => 'Dividend yield',
  fcfYield: (sbc) => `${sbc ? 'FCF-SBC' : 'FCF'} yield`,
  grossMargin: () => 'Gross margin',
  cashConversion: (sbc) => (sbc ? '(FCF − SBC) / Net Income' : 'FCF / Net Income'),
};

/**
 * ⚠⚠ THE SAME DUTCH TERM FOR THE SAME THING AS THE `Tables` TAB USES, AND IT IS ENFORCED. Both
 * surfaces live in the same modal, one summarising the other, so "vrije kasstroom per aandeel" and
 * "rendement op geïnvesteerd vermogen" have to be the identical phrase in both — a summary that
 * renames the rows it summarises is a summary of something else. `longEquityCopy.test` asserts the
 * three overlapping pairs, which is what caught it when the Tables chips gained a " CAGR" suffix
 * the headings here do not carry: `nl.chip.fcfCagr` must equal this heading exactly, so the CHIP is
 * the short name and the row LABEL is the one that says CAGR. See `tablesCopy`.
 *
 * ⚠ `SBC` SURVIVES AS A SHORT MARKER where spelling it out would double a heading's length
 * ("na aandelenbeloning"). It is the one abbreviation kept, and it is kept because the checkbox
 * that drives these three headings is itself labelled `SBC correction` in the tab's control row —
 * so the heading names the control the reader just clicked.
 */
const nl: ChartTitles = {
  sharePrice: () => 'Aandelenkoers',
  epsNri: () => 'Winst per aandeel (excl. bijzondere posten)',
  revenue: () => 'Omzet',
  fcfPs: () => 'Vrije kasstroom per aandeel',
  shares: () => 'Uitstaande aandelen',
  fcfMargin: (sbc) => (sbc ? 'Vrije kasstroom-marge na SBC' : 'Vrije kasstroom-marge'),
  croic: () => 'Kasrendement op kapitaal',
  roic: () => 'Rendement op geïnvesteerd vermogen',
  debtAssets: () => 'Schuld / activa excl. goodwill',
  interestBurden: () => 'Rente / bedrijfsresultaat',
  sbcOcf: () => 'Aandelenbeloning / operationele kasstroom',
  investedCapital: () => 'Geïnvesteerd vermogen',
  capexMargin: () => 'Investeringen / omzet',
  dividendYield: () => 'Dividendrendement',
  fcfYield: (sbc) => (sbc ? 'Vrije kasstroom-rendement na SBC' : 'Vrije kasstroom-rendement'),
  grossMargin: () => 'Brutomarge',
  cashConversion: (sbc) => (sbc
    ? '(Vrije kasstroom − SBC) / nettowinst'
    : 'Vrije kasstroom / nettowinst'),
};

export const CHART_TITLES: Record<Lang, ChartTitles> = { en, nl };

/** One card's heading. `sbc` matters to three of them; passing it always is the cheaper rule. */
export function chartTitle(lang: Lang, key: ChartKey, sbc = false): string {
  return CHART_TITLES[lang][key](sbc);
}

/**
 * WHAT EACH CHART ON THE `Graphs` TAB IS, AND THE CAVEAT THAT COMES WITH IT.
 *
 * ⚠⚠ ASKED FOR ON THE HEADING, WHICH IS WHERE THE READER IS (2026-09-03: "each graph should have
 * an info icon explaining what it is that we are viewing, and caveats to keep in mind"). Every one
 * of these cards ALREADY had an `AspectCard` — on its stat tile, explaining the AVERAGE. So a
 * reader who wanted to know what the LINE was had to hover a number to find out, and the caveat
 * that decides whether the line means anything at all (a bank has no gross profit; a loss-making
 * year has no cash conversion) sat under a figure rather than under the chart it disqualifies.
 *
 * ⚠⚠ IT COVERS ALL SEVENTEEN, AND THE FIRST CUT COVERED TWELVE. The five growth cards were left
 * out on the belief that `MetricGrowthCard` rendered its own heading tip — it does not. The tip in
 * that file hangs off the MEMBER-COUNT line, which renders only where members were actually
 * withheld, so Share price, Revenue and Shares outstanding had no ⓘ at all and EPS and FCF/share
 * had one only on the books where the positives-only filter had dropped somebody. Reported as
 * exactly that: "Share price doesn't have it, EPS doesn't have it."
 *
 * ⚠ THE CAVEAT IS THE POINT, NOT THE DEFINITION. `what` says what is plotted in one line; `how` is
 * the thing that would otherwise be learned by misreading the chart once — which years are missing
 * and why, what a blank means, which businesses the ratio simply does not describe.
 *
 * ⚠ NO WARNING GLYPHS AND NO UNICODE MATHS IN THESE STRINGS. They render in an `AspectCard`, and
 * every card on this tab is a file `tooltipStyle.test.ts` already holds to the Active Share shape.
 * The copy living in this module rather than in the JSX does not exempt it — it only puts it out
 * of that scanner's reach, which is a reason to be careful rather than a licence.
 */
export type ChartInfo = { what: string; where: string; how: string };
/** ⚠ EVERY `ChartKey`, so a card cannot be added to the tab without an explanation: the Record is
 *  exhaustive, and a new key fails `tsc` here rather than shipping a heading with no ⓘ. */
export type ChartInfos = Record<ChartKey, (sbc: boolean) => ChartInfo>;

/** The book's side of every card is a value-weighted blend of its holdings — said once, here. */
const BLEND_EN = 'GuruFocus statements, per fiscal year. For a book, each year is a value-weighted '
  + 'blend of the holdings that report the line.';
const BLEND_NL = 'GuruFocus-jaarrekeningen, per boekjaar. Voor een portefeuille is elk jaar een '
  + 'naar waarde gewogen mix van de posities die de regel rapporteren.';

/**
 * ⚠⚠ WHAT A LEVEL CARD ACTUALLY PLOTS, AND IT IS NOT THE LEVEL. Both lines are INDEXED TO 100 at
 * the first year they share, on a log axis — so what is being compared is growth, and the two
 * curves are directly comparable however far apart the underlying figures are. The hover is the
 * ACTUAL value. Said once here because it is true of all four level cards, and because it is the
 * single thing a reader has to know before the chart means anything.
 */
const INDEXED_EN = 'Both lines are indexed to 100 at the first year they share, on a log axis, so '
  + 'only the growth is compared. Hover any point for the actual figure.';
const INDEXED_NL = 'Beide lijnen staan op 100 in het eerste jaar dat ze delen, op een log-as, dus '
  + 'alleen de groei wordt vergeleken. Beweeg over een punt voor het werkelijke cijfer.';

const infoEn: ChartInfos = {
  sharePrice: () => ({
    what: 'The share price against the benchmark, both started at 100 — who grew harder, not who costs more.',
    where: 'Fiscal year-end closes from GuruFocus, in the company’s reporting currency.',
    how: `${INDEXED_EN} It is a price line: dividends are not in it, so a high payer grows slower here than a holder of it earned.`,
  }),
  epsNri: () => ({
    what: 'Earnings per share excluding non-recurring items, against the benchmark, both started at 100.',
    where: 'GuruFocus statements, per fiscal year; the dotted leg is the analyst consensus.',
    how: `${INDEXED_EN} Drawn only from companies whose EPS is positive in every period, estimates included — the count under the title says how many that is, and the loss-makers it drops are not a random few.`,
  }),
  revenue: () => ({
    what: 'Revenue against the benchmark, both started at 100.',
    where: 'GuruFocus statements, per fiscal year. For a book, the euros are summed across holdings.',
    how: `${INDEXED_EN} This is the one line built by adding the euros up rather than averaging each company’s growth rate, so a big holding moves it in proportion to its revenue, not its valuation.`,
  }),
  fcfPs: () => ({
    what: 'Free cash flow per share against the benchmark, both started at 100.',
    where: 'GuruFocus statements, per fiscal year.',
    how: `${INDEXED_EN} Drawn only from companies positive in every period, so cash-burners, recoveries and banks whose cash flow swings on deposit flows are absent — the count under the title says how many remain.`,
  }),
  shares: () => ({
    what: 'The share count against the benchmark, both started at 100 — buybacks fall, dilution rises.',
    where: 'GuruFocus statements, per fiscal year: the diluted average share count.',
    how: `${INDEXED_EN} It is the denominator under every per-share card here, so a falling line lifts EPS and FCF per share without the business earning more.`,
  }),
  fcfMargin: (sbc) => ({
    what: `How much of each euro of revenue is left as free cash flow${sbc ? ', after stock compensation' : ''}.`,
    where: BLEND_EN,
    how: 'Stock comp is a non-cash add-back to free cash flow, so the SBC-corrected line is the '
      + 'truer cash margin. A loss-making year plots below zero rather than dropping out.',
  }),
  croic: () => ({
    what: 'The free cash flow thrown off per euro of long-term capital employed.',
    where: BLEND_EN,
    how: 'After capex and after interest, so it swings with the investment cycle and charges '
      + 'leverage twice. Blank where an issuer does not split current from non-current liabilities '
      + '(a bank, Berkshire): the capital base is then undefined, not equity alone.',
  }),
  roic: () => ({
    what: 'The after-tax operating profit earned per euro of invested capital.',
    where: 'GuruFocus publishes this one directly; nothing here re-derives it.',
    how: 'Read it as the vendor states it. Deriving it would mean choosing a NOPAT line and a '
      + 'capital base, i.e. a bespoke ratio under a name every reader already defines for '
      + 'themselves, so the drill-down shows one figure per company per year and no workings.',
  }),
  debtAssets: () => ({
    what: 'Long-term debt measured against tangible assets, per fiscal year.',
    where: BLEND_EN,
    how: 'Goodwill is stripped out of assets, so leverage is measured against assets that can '
      + 'actually fund it. Lower is less levered.',
  }),
  interestBurden: () => ({
    what: 'The share of operating profit that goes on interest.',
    where: BLEND_EN,
    how: 'Operating profit is GuruFocus’s Operating Income line. A heavily-levered company reads '
      + 'high; a year with no operating profit has no meaningful ratio and is left out.',
  }),
  sbcOcf: () => ({
    what: 'The share of operating cash flow paid out in stock rather than in cash.',
    where: BLEND_EN,
    how: 'Stock comp is a non-cash expense added back into operating cash flow, so a high share '
      + 'means much of the reported cash generation is really dilution. Lower is better.',
  }),
  investedCapital: () => ({
    what: 'The long-term capital funding the business: non-current liabilities plus total equity.',
    where: BLEND_EN,
    how: 'It is the base the Cash-return card divides free cash flow by, so the two move together. '
      + 'A rising line means the business is soaking up more capital; the fit says how steadily.',
  }),
  capexMargin: () => ({
    what: 'The share of each euro of revenue reinvested in property, plant and intangibles.',
    where: BLEND_EN,
    how: 'This is capital intensity, not waste: an asset-heavy business reads high by nature. '
      + 'Compare it against peers rather than against zero.',
  }),
  dividendYield: () => ({
    what: 'The dividend paid per share, against the share price.',
    where: BLEND_EN,
    how: 'A company that pays nothing counts as 0%; one with no dividend line at all is left out '
      + 'and the year renormalises over the rest. On Daily the payout stays flat between fiscal '
      + 'periods while the price moves, so the line is the price moving.',
  }),
  fcfYield: (sbc) => ({
    what: `The free cash flow${sbc ? ' after stock compensation' : ''} a buyer earns per euro of market value.`,
    where: BLEND_EN,
    how: 'Higher is cheaper for the cash it generates. On Daily the cash figure stays flat between '
      + 'fiscal periods while the market cap moves, so the line is the price moving.',
  }),
  grossMargin: () => ({
    what: 'What is left of each sale after the direct cost of making it.',
    where: BLEND_EN,
    how: 'Higher suggests pricing power. A bank has no gross profit line at all, so a book holding '
      + 'banks is averaged over the rest and the coverage figure says how much of it that is.',
  }),
  cashConversion: (sbc) => ({
    what: `How much of the reported profit turned into free cash flow${sbc ? ', after stock compensation' : ''}.`,
    where: BLEND_EN,
    how: '100% is break-even, not a ceiling. A loss-making year has no conversion at all, so it is '
      + 'a gap in the line rather than a negative reading.',
  }),
};

const infoNl: ChartInfos = {
  sharePrice: () => ({
    what: 'De aandelenkoers tegenover de benchmark, beide op 100 gestart — wie harder groeit, niet wie duurder is.',
    where: 'Slotkoersen per boekjaareinde van GuruFocus, in de rapportagevaluta van de onderneming.',
    how: `${INDEXED_NL} Het is een koerslijn: dividenden zitten er niet in, dus een hoge uitkeerder groeit hier trager dan wie hem hield verdiende.`,
  }),
  epsNri: () => ({
    what: 'Winst per aandeel exclusief bijzondere posten tegenover de benchmark, beide op 100 gestart.',
    where: 'GuruFocus-jaarrekeningen per boekjaar; de stippellijn is de analistenconsensus.',
    how: `${INDEXED_NL} Alleen getekend uit ondernemingen met een positieve winst in elke periode, ramingen inbegrepen — het aantal onder de titel zegt hoeveel dat er zijn.`,
  }),
  revenue: () => ({
    what: 'De omzet tegenover de benchmark, beide op 100 gestart.',
    where: 'GuruFocus-jaarrekeningen per boekjaar. Voor een portefeuille worden de euro’s opgeteld.',
    how: `${INDEXED_NL} Dit is de enige lijn die de euro’s optelt in plaats van de groeivoeten te middelen, dus een grote positie telt naar omzet mee, niet naar beurswaarde.`,
  }),
  fcfPs: () => ({
    what: 'Vrije kasstroom per aandeel tegenover de benchmark, beide op 100 gestart.',
    where: 'GuruFocus-jaarrekeningen, per boekjaar.',
    how: `${INDEXED_NL} Alleen uit ondernemingen die in elke periode positief zijn, dus kasverbranders, herstelgevallen en banken ontbreken — het aantal onder de titel zegt hoeveel er overblijven.`,
  }),
  shares: () => ({
    what: 'Het aantal aandelen tegenover de benchmark, beide op 100 gestart — inkoop daalt, verwatering stijgt.',
    where: 'GuruFocus-jaarrekeningen per boekjaar: het verwaterde gemiddelde aantal aandelen.',
    how: `${INDEXED_NL} Het is de noemer onder elke kaart per aandeel, dus een dalende lijn tilt de winst en kasstroom per aandeel op zonder dat er meer wordt verdiend.`,
  }),
  fcfMargin: (sbc) => ({
    what: `Hoeveel van elke euro omzet overblijft als vrije kasstroom${sbc ? ', na aandelenbeloning' : ''}.`,
    where: BLEND_NL,
    how: 'Aandelenbeloning is een niet-kaspost die bij de vrije kasstroom wordt opgeteld; de lijn '
      + 'na SBC is dus de zuiverdere kasmarge. Een verliesjaar staat onder nul in plaats van weg.',
  }),
  croic: () => ({
    what: 'De vrije kasstroom per euro langlopend geïnvesteerd kapitaal.',
    where: BLEND_NL,
    how: 'Na investeringen en na rente, dus het beweegt mee met de investeringscyclus en rekent '
      + 'schuld dubbel aan. Leeg wanneer een uitgevende instelling kortlopende en langlopende '
      + 'verplichtingen niet splitst (een bank, Berkshire): de kapitaalbasis is dan onbepaald.',
  }),
  roic: () => ({
    what: 'De operationele winst na belasting per euro geïnvesteerd vermogen.',
    where: 'GuruFocus publiceert dit cijfer rechtstreeks; hier wordt niets herberekend.',
    how: 'Lees het zoals de leverancier het stelt. Zelf afleiden zou betekenen dat wij een '
      + 'NOPAT-regel en een kapitaalbasis kiezen, oftewel een eigen ratio onder een naam die elke '
      + 'lezer al zelf definieert. De uitsplitsing toont daarom geen tussenstappen.',
  }),
  debtAssets: () => ({
    what: 'Langlopende schuld afgezet tegen materiële activa, per boekjaar.',
    where: BLEND_NL,
    how: 'Goodwill wordt uit de activa gehaald, zodat de hefboom wordt gemeten tegen activa die '
      + 'hem ook echt kunnen dragen. Lager is minder hefboom.',
  }),
  interestBurden: () => ({
    what: 'Het deel van het bedrijfsresultaat dat opgaat aan rente.',
    where: BLEND_NL,
    how: 'Het bedrijfsresultaat is de Operating Income-regel van GuruFocus. Een sterk gefinancierde '
      + 'onderneming leest hoog; een jaar zonder bedrijfsresultaat heeft geen zinvolle ratio.',
  }),
  sbcOcf: () => ({
    what: 'Het deel van de operationele kasstroom dat in aandelen wordt uitbetaald in plaats van in geld.',
    where: BLEND_NL,
    how: 'Aandelenbeloning is een niet-kaspost die bij de operationele kasstroom wordt opgeteld; '
      + 'een hoog aandeel betekent dus dat veel van de gerapporteerde kasstroom verwatering is.',
  }),
  investedCapital: () => ({
    what: 'Het langlopende kapitaal achter de onderneming: langlopende verplichtingen plus eigen vermogen.',
    where: BLEND_NL,
    how: 'Het is de noemer waar de kaart Kasrendement de vrije kasstroom door deelt; beide bewegen '
      + 'dus samen. Een stijgende lijn betekent dat de onderneming meer kapitaal opneemt.',
  }),
  capexMargin: () => ({
    what: 'Het deel van elke euro omzet dat opnieuw in vaste activa wordt geïnvesteerd.',
    where: BLEND_NL,
    how: 'Dit is kapitaalintensiteit, geen verspilling: een kapitaalintensief bedrijf leest van '
      + 'nature hoog. Vergelijk het met sectorgenoten, niet met nul.',
  }),
  dividendYield: () => ({
    what: 'Het uitgekeerde dividend per aandeel, afgezet tegen de koers.',
    where: BLEND_NL,
    how: 'Een onderneming die niets uitkeert telt als 0%; een zonder dividendregel valt weg en het '
      + 'jaar wordt over de rest herwogen. Bij Dagelijks blijft de uitkering vlak tussen boekjaren '
      + 'terwijl de koers beweegt, dus de lijn is de koers.',
  }),
  fcfYield: (sbc) => ({
    what: `De vrije kasstroom${sbc ? ' na aandelenbeloning' : ''} die een koper verdient per euro beurswaarde.`,
    where: BLEND_NL,
    how: 'Hoger is goedkoper voor de kasstroom die eruit komt. Bij Dagelijks blijft het kascijfer '
      + 'vlak tussen boekjaren terwijl de beurswaarde beweegt, dus de lijn is de koers.',
  }),
  grossMargin: () => ({
    what: 'Wat er van elke verkoop overblijft na de directe kostprijs ervan.',
    where: BLEND_NL,
    how: 'Hoger wijst op prijszettingsmacht. Een bank heeft helemaal geen brutowinstregel, dus een '
      + 'portefeuille met banken wordt over de rest gemiddeld; de dekkingsgraad zegt hoeveel dat is.',
  }),
  cashConversion: (sbc) => ({
    what: `Hoeveel van de gerapporteerde winst vrije kasstroom werd${sbc ? ', na aandelenbeloning' : ''}.`,
    where: BLEND_NL,
    how: '100% is het omslagpunt, geen plafond. Een verliesjaar heeft geen conversie en is dus een '
      + 'gat in de lijn in plaats van een negatieve waarde.',
  }),
};

export const CHART_INFO: Record<Lang, ChartInfos> = { en: infoEn, nl: infoNl };

/** One card's heading tip. `sbc` matters to three of them; passing it always is the cheaper rule. */
export function chartInfo(lang: Lang, key: ChartKey, sbc = false): ChartInfo {
  return CHART_INFO[lang][key](sbc);
}
