# Management dashboard tooltip review

Reviewed 30 October 2026. The import graph from `frontend/app/management-dashboard/page.tsx` contains 195 InfoTip/Provenance call sites in 38 modules. This is a conservative source inventory: it includes shared modules imported for individual exports, and counts a table-cell implementation once rather than once per rendered holding.

## Presentation

- The dashboard mounts `CompactHelpProvider`. Dialogs and React portals inherit the same presentation; other pages retain their existing tooltips.
- Start with a short definition. Already concise calculation rules stay visible. Longer methods and symbol legends are intact under **Calculation details**; click the info icon to pin the tooltip, then expand them.
- Keep actual substituted calculations visible. The numerical half of the existing `withWorked` format is extracted without changing its precision or arithmetic. Explicit blank-line-separated working in plain-text methods also remains visible.
- Keep custom tables, cash-flow calculations, dates, sources, coverage, failed-refresh warnings and refresh actions. Valuation dates and acquisition dates remain distinct. No source dates or missing values are invented.
- The monthly benchmark table remains unchanged. The per-company ACWI weight tooltip now also uses an opening/closing EUR-price table and the actual weight calculation.
- Active Share now explains the meaning of 0% and 100%, with its actual overlap check and dated inputs. Long LTM-versus-annual explanations in the growth and CAGR views are shortened.

## Inventory

All tooltip mechanisms in these dashboard dependencies were reviewed: ordinary InfoTip text, Provenance, AspectCard/AboutCard/TipCard, and caller-designed rich content. Financial input labels and useful table markup are retained.

| Module | InfoTip / Provenance call sites |
| --- | ---: |
| `frontend/app/components/PortfolioOverviewPanel.tsx` | 39 |
| `frontend/app/components/portfolios/PortfolioAnalysisModal.tsx` | 33 |
| `frontend/app/components/portfolios/DeepValuationTab.tsx` | 13 |
| `frontend/app/components/portfolios/ReverseDcfPanel.tsx` | 12 |
| `frontend/app/components/portfolios/TablesTab.tsx` | 9 |
| `frontend/app/components/PortfoliosPanel.tsx` | 8 |
| `frontend/app/components/portfolios/PriceTargetCalculator.tsx` | 8 |
| `frontend/app/components/portfolios/QuickValuationTab.tsx` | 8 |
| `frontend/app/components/portfolios/MetricGrowthCard.tsx` | 7 |
| `frontend/app/components/portfolios/PortfolioFundamentalModal.tsx` | 6 |
| `frontend/app/components/portfolios/MonthlyReportPanel.tsx` | 5 |
| `frontend/app/components/portfolios/FcfSbcYieldInputsModal.tsx` | 5 |
| `frontend/app/components/portfolios/CagrTable.tsx` | 5 |
| `frontend/app/components/portfolios/ConcentrationView.tsx` | 4 |
| `frontend/app/components/portfolios/ActiveSharePanel.tsx` | 4 |
| `frontend/app/components/portfolios/InvestedCapitalCard.tsx` | 4 |
| `frontend/app/components/portfolios/MultipleHistoryChart.tsx` | 3 |
| `frontend/app/components/portfolios/CorrelationView.tsx` | 2 |
| `frontend/app/components/portfolios/HoldingsRevenueModal.tsx` | 2 |
| `frontend/app/components/portfolios/DividendYieldCard.tsx` | 2 |
| `frontend/lib/provenance.tsx` | 1 |
| `frontend/app/components/portfolios/TrackingErrorView.tsx` | 1 |
| `frontend/app/components/portfolios/VolatilityView.tsx` | 1 |
| `frontend/app/components/portfolios/DrawdownView.tsx` | 1 |
| `frontend/app/components/portfolios/BookReturnChart.tsx` | 1 |
| `frontend/app/components/portfolios/GraphCoverage.tsx` | 1 |
| `frontend/app/components/portfolios/CardHeading.tsx` | 1 |
| `frontend/app/components/portfolios/MarginCard.tsx` | 1 |
| `frontend/app/components/portfolios/CashReturnCard.tsx` | 1 |
| `frontend/app/components/portfolios/DebtRatioCard.tsx` | 1 |
| `frontend/app/components/portfolios/InterestBurdenCard.tsx` | 1 |
| `frontend/app/components/portfolios/SbcOcfCard.tsx` | 1 |
| `frontend/app/components/portfolios/CapexMarginCard.tsx` | 1 |
| `frontend/app/components/portfolios/GrossMarginCard.tsx` | 1 |
| `frontend/app/components/portfolios/CashConversionCard.tsx` | 1 |
| `frontend/app/components/portfolios/FcfSbcYieldCard.tsx` | 1 |
| `frontend/lib/tipCard.tsx` | 0 |
| `frontend/app/components/InfoTip.tsx` | 0 |

## Validation

Unit checks cover dashboard scoping, numerical working, preserved custom tables, source versus fetch dates, failed-refresh warnings, short methods and nested dialog language. The full frontend suite, TypeScript and targeted lint pass. No browser or live-database tests were used. These changes alter explanation presentation, not financial calculations or refresh behaviour.
