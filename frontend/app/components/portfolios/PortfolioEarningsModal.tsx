"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { apiFetch } from "../../../lib/apiFetch";
import { API_URL } from "../../../lib/apiUrl";
import { startJob } from "../../../lib/stores/jobs";
import PanelDialog from "./PanelDialog";
import PortfolioFundamentalsRefresh from "./PortfolioFundamentalsRefresh";
import type { Basket } from "./types";

type Metric = {
  metric_code: string;
  target_date: string;
  numeric_value: number | null;
  is_prediction?: boolean | null;
  recorded_at?: string | null;
};
type Company = {
  company_id: number;
  isin: string;
  name: string;
  currency?: string | null;
  metrics: Metric[];
};
type Payload = { rows: Company[] };
type Quarter = { date: string; value: number; metricCode: string };
type Surprise = { text: string; positive: boolean; consensus: number | null };
const REVENUE_CODES = new Set([
  "quarterly__Income Statement__Revenue",
  "quarterly__income_statement__Revenue",
]);
const EPS_CODES = new Set([
  "quarterly__Per Share Data__EPS without NRI",
  "quarterly__per_share_data__EPS without NRI",
  "quarterly__per_share_data_array__EPS without NRI",
  "quarterly__Per Share Data__Earnings per Share (Diluted)",
  "quarterly__per_share_data__Earnings per Share (Diluted)",
]);

function quarterlySeries(
  company: Company,
  codes: ReadonlySet<string>,
): Quarter[] {
  const byDate = new Map<string, Metric>();
  for (const metric of company.metrics) {
    if (
      !codes.has(metric.metric_code) ||
      metric.numeric_value == null ||
      metric.is_prediction
    )
      continue;
    const previous = byDate.get(metric.target_date);
    if (!previous || (metric.recorded_at ?? "") > (previous.recorded_at ?? ""))
      byDate.set(metric.target_date, metric);
  }
  return [...byDate.values()]
    .map((metric) => ({
      date: metric.target_date,
      value: metric.numeric_value!,
      metricCode: metric.metric_code,
    }))
    .sort((a, b) => b.date.localeCompare(a.date));
}

function estimateSeries(
  company: Company,
  codes: ReadonlySet<string>,
): Quarter[] {
  const byDate = new Map<string, Metric>();
  for (const metric of company.metrics) {
    if (
      !codes.has(metric.metric_code) ||
      metric.numeric_value == null ||
      !metric.is_prediction
    )
      continue;
    const previous = byDate.get(metric.target_date);
    if (!previous || (metric.recorded_at ?? "") > (previous.recorded_at ?? ""))
      byDate.set(metric.target_date, metric);
  }
  return [...byDate.values()]
    .map((metric) => ({
      date: metric.target_date,
      value: metric.numeric_value!,
      metricCode: metric.metric_code,
    }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

const REVENUE_ESTIMATE_CODES = new Set(["quarterly_revenue_estimate"]);
const EPS_ESTIMATE_CODES = new Set([
  "quarterly_per_share_eps_estimate",
  "quarterly_eps_nri_estimate",
]);
function historicalSurprise(
  company: Company,
  date: string,
  actual: number,
  metric: "revenue_estimate" | "per_share_eps_estimate" | "eps_nri_estimate",
  label: string,
  value: (value: number) => string,
): Surprise | null {
  const prefix = `quarterly_estimate_history__${metric}__`;
  const values = new Map(
    company.metrics
      .filter(
        (row) =>
          row.target_date === date &&
          row.numeric_value != null &&
          row.metric_code.startsWith(prefix),
      )
      .map((row) => [row.metric_code.slice(prefix.length), row.numeric_value!]),
  );
  const consensus = values.get("consensus");
  const providerActual = values.get("actual");
  // A statement's line item can use a different definition from the vendor's
  // estimate series (Adyen's half-year revenue is one example). Never call a
  // result a Beat/Missed unless both actuals describe the same value.
  if (
    consensus == null ||
    providerActual == null ||
    Math.abs(providerActual - actual) > Math.max(0.01, Math.abs(actual) * 0.005)
  )
    return null;
  const difference = actual - consensus;
  const pct = consensus === 0 ? null : (difference / Math.abs(consensus)) * 100;
  return {
    text: `${difference >= 0 ? "Beat" : "Missed"} ${label} by ${difference >= 0 ? "+" : ""}${value(Math.abs(difference))}${pct == null ? "" : ` (${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%)`}${consensus == null ? "" : ` vs ${value(consensus)} consensus`}`,
    positive: difference >= 0,
    consensus: consensus ?? null,
  };
}

function lastYear(quarter: Quarter, all: Quarter[]): Quarter | null {
  return (
    all.find(
      (candidate) =>
        candidate.date ===
        `${Number(quarter.date.slice(0, 4)) - 1}${quarter.date.slice(4)}`,
    ) ?? null
  );
}
function quarterLabel(date: string): string {
  const [year, month] = date.split("-");
  return `Q${Math.ceil(Number(month) / 3)} ${year}`;
}

/** Some companies (for example Adyen) report and receive consensus only half-yearly. */
function estimatePeriodLabel(date: string, estimates: Quarter[]): string {
  const dates = [...new Set(estimates.map((estimate) => estimate.date))].sort();
  const monthNumbers = dates.map((value) => {
    const [year, month] = value.split("-").map(Number);
    return year * 12 + month;
  });
  const shortestGap = monthNumbers.slice(1).reduce<number | null>(
    (smallest, month, index) => {
      const gap = month - monthNumbers[index];
      return smallest == null || gap < smallest ? gap : smallest;
    },
    null,
  );
  if (shortestGap != null && shortestGap >= 5) {
    const [year, month] = date.split("-").map(Number);
    return `H${month <= 6 ? 1 : 2} ${year}`;
  }
  return quarterLabel(date);
}
function revenue(value: number, currency?: string | null): string {
  return `${new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(value)}m${currency ? ` ${currency}` : ""}`;
}
function eps(value: number, currency?: string | null): string {
  return `${value.toFixed(2)}${currency ? ` ${currency}` : ""}`;
}
function yoy(
  label: string,
  growth: number | null,
  previous: Quarter | null,
  value: (number: number) => string,
): string {
  return growth == null || !previous
    ? `${label} YoY -`
    : `${label} YoY ${growth >= 0 ? "+" : ""}${growth.toFixed(1)}% vs ${value(previous.value)}`;
}

export default function PortfolioEarningsModal({
  name,
  portfolioId,
  basket,
  bookPortfolio,
  onClose,
}: {
  name: string;
  portfolioId?: number;
  basket?: Basket;
  bookPortfolio?: string;
  onClose: () => void;
}) {
  const [data, setData] = useState<Payload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [revision, setRevision] = useState(0);
  const historyRequested = useRef<string | null>(null);
  useEffect(() => {
    const controller = new AbortController();
    void (async () => {
      try {
        const response = await apiFetch(
          `${API_URL}/api/earnings/portfolio-company-metrics`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(
              bookPortfolio
                ? { book_portfolio: bookPortfolio, earnings_only: true }
                : portfolioId != null
                  ? { portfolio_id: portfolioId, earnings_only: true }
                  : {
                      holdings: basket?.holdings ?? [],
                      basket_label: basket?.label ?? name,
                      earnings_only: true,
                    },
            ),
            signal: controller.signal,
          },
        );
        const body = (await response.json().catch(() => null)) as
          Payload | { detail?: string } | null;
        if (!response.ok)
          throw new Error(
            (body as { detail?: string } | null)?.detail ??
              `HTTP ${response.status}`,
          );
        if (!controller.signal.aborted) setData(body as Payload);
      } catch (reason) {
        if (!controller.signal.aborted)
          setError(reason instanceof Error ? reason.message : String(reason));
      }
    })();
    return () => controller.abort();
  }, [basket, bookPortfolio, name, portfolioId, revision]);

  useEffect(() => {
    const missing = (data?.rows ?? []).filter((company) =>
      company.metrics.some(
        (metric) =>
          EPS_CODES.has(metric.metric_code) &&
          !metric.is_prediction &&
          !company.metrics.some(
            (history) =>
              history.metric_code ===
                `quarterly_estimate_history__${metric.metric_code.includes("EPS without NRI") ? "eps_nri_estimate" : "per_share_eps_estimate"}__consensus` &&
              history.target_date === metric.target_date,
          ),
      ),
    );
    if (!missing.length) return;
    const key = missing.map((company) => company.company_id).sort().join(",");
    if (historyRequested.current === key) return;
    historyRequested.current = key;
    const body = bookPortfolio
      ? { book_portfolio: bookPortfolio }
      : portfolioId != null
        ? { portfolio_id: portfolioId }
        : {
            holdings: basket?.holdings ?? [],
            basket_label: basket?.label ?? name,
          };
    void startJob(
      `${API_URL}/api/earnings/portfolio-historical-estimates/ingest/job`,
      `${name}: historical earnings estimates`,
      {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      },
    ).then(({ done }) => done).then((job) => {
      if (job.status !== "failed") setRevision((value) => value + 1);
    }).catch(() => {
      // The table remains useful without consensus history; the job toast carries the error.
    });
  }, [basket, bookPortfolio, data, name, portfolioId]);

  const rows = useMemo(
    () =>
      (data?.rows ?? [])
        .map((company) => ({
          company,
          revenueQuarters: quarterlySeries(company, REVENUE_CODES),
          epsQuarters: quarterlySeries(company, EPS_CODES),
          revenueEstimates: estimateSeries(company, REVENUE_ESTIMATE_CODES),
          epsEstimates: estimateSeries(company, EPS_ESTIMATE_CODES),
        }))
        .sort((a, b) => a.company.name.localeCompare(b.company.name)),
    [data],
  );
  return (
    <PanelDialog onClose={onClose} labelledBy="portfolio-earnings-title">
      <div className="flex h-full min-h-0 flex-col rounded-xl border border-neutral-800/40 bg-card shadow-xl">
        <header className="flex shrink-0 items-center gap-4 border-b border-neutral-800/40 px-5 py-4">
          <div className="min-w-0">
            <h3
              id="portfolio-earnings-title"
              className="text-lg font-semibold text-fg-strong"
            >
              Earnings
            </h3>
            <p className="truncate text-sm text-fg-muted">{name}</p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="ml-auto rounded-md border border-neutral-700 bg-page px-3 py-1.5 text-sm font-medium text-fg-muted hover:bg-overlay/5 hover:text-fg-strong"
          >
            Close
          </button>
        </header>
        <div className="min-h-0 flex-1 overflow-auto px-5 py-4">
          {!data && !error && (
            <p className="py-16 text-center text-sm text-fg-muted">
              Loading company earnings...
            </p>
          )}
          {error && (
            <p className="rounded-lg border border-neg-500/30 bg-neg-500/10 p-3 text-sm text-neg-300">
              {error}
            </p>
          )}
          {data && (
            <div className="overflow-auto border border-neutral-800/50">
              <table className="min-w-[1100px] w-full border-separate border-spacing-0 text-sm">
                <thead className="sticky top-0 z-10 bg-page text-left text-xs text-fg-faint">
                  <tr>
                    <th className="sticky left-0 z-20 w-80 min-w-80 bg-page px-3 py-3 font-medium">
                      Company
                    </th>
                    <th className="px-3 py-3 font-medium">Two quarters ago</th>
                    <th className="px-3 py-3 font-medium">Previous quarter</th>
                    <th className="border-r-2 border-neutral-700/70 px-3 py-3 font-medium">
                      Latest reported quarter
                    </th>
                    <th className="bg-neutral-800/35 px-3 py-3 font-medium text-fg-muted">
                      Next quarter estimate
                    </th>
                    <th className="bg-neutral-800/35 px-3 py-3 font-medium text-fg-muted">
                      Following quarter estimate
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map(
                    ({
                      company,
                      revenueQuarters,
                      epsQuarters,
                      revenueEstimates,
                      epsEstimates,
                    }) => (
                      <tr
                        key={company.company_id}
                        className="group transition-colors hover:bg-accent-500/10"
                      >
                        <td className="sticky left-0 z-10 border-b border-neutral-800/50 bg-page px-3 py-3 transition-colors group-hover:bg-accent-500/10">
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              <div className="truncate font-medium text-fg-strong">
                                {company.name}
                              </div>
                              <div className="mt-0.5 font-mono text-[11px] text-fg-faint">
                                {company.isin}
                              </div>
                            </div>
                            <PortfolioFundamentalsRefresh
                              scope={{
                                kind: "company",
                                isin: company.isin,
                                name: company.name,
                              }}
                              feeds="statements_estimates"
                              allPeriods
                              compact
                              broadcast={false}
                              showNote={false}
                              label="Refresh"
                              jobTitle={`${company.name}: earnings`}
                              onDone={() => setRevision((value) => value + 1)}
                            />
                          </div>
                        </td>
                        {[2, 1, 0].map((index) => {
                          const revenuePoint = revenueQuarters[index];
                          const epsPoint = epsQuarters[index];
                          const revenuePrior = revenuePoint
                            ? lastYear(revenuePoint, revenueQuarters)
                            : null;
                          const epsPrior = epsPoint
                            ? lastYear(epsPoint, epsQuarters)
                            : null;
                          const revenueGrowth =
                            revenuePoint &&
                            revenuePrior &&
                            revenuePrior.value > 0
                              ? (revenuePoint.value / revenuePrior.value - 1) *
                                100
                              : null;
                          const epsGrowth =
                            epsPoint && epsPrior && epsPrior.value !== 0
                              ? (epsPoint.value / Math.abs(epsPrior.value) -
                                  1) *
                                100
                              : null;
                          const revenueSurprise = revenuePoint
                            ? historicalSurprise(
                                company,
                                revenuePoint.date,
                                revenuePoint.value,
                                "revenue_estimate",
                                "revenue",
                                (value) => revenue(value, company.currency),
                              )
                            : null;
                          const epsSurprise = epsPoint
                            ? historicalSurprise(
                                company,
                                epsPoint.date,
                                epsPoint.value,
                                epsPoint.metricCode.includes("EPS without NRI")
                                  ? "eps_nri_estimate"
                                  : "per_share_eps_estimate",
                                "EPS",
                                (value) => eps(value, company.currency),
                              )
                            : null;
                          return (
                            <td
                              key={index}
                              className={`border-b border-neutral-800/50 p-0 tabular-nums ${revenuePoint || epsPoint ? "align-top" : "align-middle text-center"} ${index === 0 ? "border-r-2 border-neutral-700/70" : ""}`}
                            >
                              {revenuePoint || epsPoint ? (
                                <>
                                  <div className="px-3 pt-3 text-xs text-fg-subtle">
                                    {quarterLabel(
                                      (revenuePoint ?? epsPoint!).date,
                                    )}{" "}
                                    - ended {(revenuePoint ?? epsPoint!).date}
                                  </div>
                                  {revenuePoint ? (
                                    <>
                                      <div className="mt-1 flex min-h-5 items-center gap-2 px-3 font-medium text-fg-strong">
                                        Revenue{" "}
                                        {revenue(
                                          revenuePoint.value,
                                          company.currency,
                                        )}
                                        {revenueSurprise && (
                                          <span
                                            className={`inline-flex rounded px-1.5 py-0.5 text-xs font-medium ${revenueSurprise.positive ? "bg-pos-500/15 text-pos-400" : "bg-neg-500/15 text-neg-400"}`}
                                          >
                                            {revenueSurprise.positive
                                              ? "Beat"
                                              : "Missed"}{" "}
                                            {revenueSurprise.consensus == null
                                              ? revenueSurprise.text
                                              : revenue(
                                                  revenueSurprise.consensus,
                                                  company.currency,
                                                )}
                                          </span>
                                        )}
                                      </div>
                                      <div
                                        className={`mt-1 min-h-5 px-3 text-xs ${revenueGrowth == null ? "text-fg-faint" : revenueGrowth >= 0 ? "text-pos-400" : "text-neg-400"}`}
                                      >
                                        {yoy(
                                          "Revenue",
                                          revenueGrowth,
                                          revenuePrior,
                                          (value) =>
                                            revenue(value, company.currency),
                                        )}
                                      </div>
                                      <div className="mt-1 min-h-5" />
                                    </>
                                  ) : (
                                    <div className="mt-1 px-3 text-fg-faint">
                                      Revenue -
                                    </div>
                                  )}
                                  {epsPoint ? (
                                    <>
                                      <div className="mt-2 flex min-h-5 items-center gap-2 px-3 pt-2 font-medium text-fg-strong">
                                        EPS{" "}
                                        {eps(epsPoint.value, company.currency)}
                                        {epsSurprise && (
                                          <span
                                            className={`inline-flex rounded px-1.5 py-0.5 text-xs font-medium ${epsSurprise.positive ? "bg-pos-500/15 text-pos-400" : "bg-neg-500/15 text-neg-400"}`}
                                          >
                                            {epsSurprise.positive
                                              ? "Beat"
                                              : "Missed"}{" "}
                                            {epsSurprise.consensus == null
                                              ? epsSurprise.text
                                              : eps(
                                                  epsSurprise.consensus,
                                                  company.currency,
                                                )}
                                          </span>
                                        )}
                                      </div>
                                      <div
                                        className={`mt-1 min-h-5 px-3 text-xs ${epsGrowth == null ? "text-fg-faint" : epsGrowth >= 0 ? "text-pos-400" : "text-neg-400"}`}
                                      >
                                        {yoy(
                                          "EPS",
                                          epsGrowth,
                                          epsPrior,
                                          (value) =>
                                            eps(value, company.currency),
                                        )}
                                      </div>
                                      <div className="mt-1 min-h-5" />
                                    </>
                                  ) : (
                                    <div className="mt-2 px-3 pt-2 text-fg-faint">
                                      EPS -
                                    </div>
                                  )}
                                </>
                              ) : (
                                <span className="block px-3 text-fg-faint">
                                  No reported quarter
                                </span>
                              )}
                            </td>
                          );
                        })}
                        {[0, 1].map((index) => {
                          const latestDate = (
                            revenueQuarters[0] ?? epsQuarters[0]
                          )?.date;
                          const date = [
                            ...new Set(
                              [...revenueEstimates, ...epsEstimates].map(
                                (point) => point.date,
                              ),
                            ),
                          ]
                            .filter(
                              (candidate) =>
                                !latestDate || candidate > latestDate,
                            )
                            .sort()[index];
                          const revenueEstimate = revenueEstimates.find(
                            (point) => point.date === date,
                          );
                          const epsEstimate = epsEstimates.find(
                            (point) => point.date === date,
                          );
                          const revenueEstimatePrior = revenueEstimate
                            ? (lastYear(revenueEstimate, revenueEstimates) ??
                              lastYear(revenueEstimate, revenueQuarters))
                            : null;
                          const epsEstimatePrior = epsEstimate
                            ? (lastYear(epsEstimate, epsEstimates) ??
                              lastYear(epsEstimate, epsQuarters))
                            : null;
                          const revenueEstimateGrowth =
                            revenueEstimate &&
                            revenueEstimatePrior &&
                            revenueEstimatePrior.value > 0
                              ? (revenueEstimate.value /
                                  revenueEstimatePrior.value -
                                  1) *
                                100
                              : null;
                          const epsEstimateGrowth =
                            epsEstimate &&
                            epsEstimatePrior &&
                            epsEstimatePrior.value !== 0
                              ? (epsEstimate.value /
                                  Math.abs(epsEstimatePrior.value) -
                                  1) *
                                100
                              : null;
                          return (
                            <td
                              key={`estimate-${index}`}
                              className="border-b border-neutral-800/50 bg-neutral-900/25 p-0 align-top tabular-nums"
                            >
                              {date ? (
                                <>
                                  <div className="px-3 pt-3 text-xs text-fg-subtle">
                                    {estimatePeriodLabel(
                                      date,
                                      [...revenueEstimates, ...epsEstimates],
                                    )} consensus
                                  </div>
                                  <div className="px-3">
                                    {revenueEstimate ? (
                                      <div className="mt-1 text-fg-soft">
                                        Revenue{" "}
                                        {revenue(
                                          revenueEstimate.value,
                                          company.currency,
                                        )}
                                      </div>
                                    ) : (
                                      <div className="mt-1 text-fg-faint">
                                        Revenue -
                                      </div>
                                    )}
                                    <div
                                      className={`mt-1 min-h-5 text-xs ${revenueEstimateGrowth == null ? "text-fg-faint" : revenueEstimateGrowth >= 0 ? "text-pos-400" : "text-neg-400"}`}
                                    >
                                      {revenueEstimate &&
                                        yoy(
                                          "Revenue",
                                          revenueEstimateGrowth,
                                          revenueEstimatePrior,
                                          (value) =>
                                            revenue(value, company.currency),
                                        )}
                                    </div>
                                    <div className="mt-1 min-h-5" />
                                  </div>
                                  <div className="mt-2 px-3 pt-2">
                                    {epsEstimate ? (
                                      <div className="text-fg-soft">
                                        EPS{" "}
                                        {eps(epsEstimate.value, company.currency)}
                                      </div>
                                    ) : (
                                      <div className="text-fg-faint">EPS -</div>
                                    )}
                                    <div
                                      className={`mt-1 min-h-5 text-xs ${epsEstimateGrowth == null ? "text-fg-faint" : epsEstimateGrowth >= 0 ? "text-pos-400" : "text-neg-400"}`}
                                    >
                                      {epsEstimate &&
                                        yoy(
                                          "EPS",
                                          epsEstimateGrowth,
                                          epsEstimatePrior,
                                          (value) =>
                                            eps(value, company.currency),
                                        )}
                                    </div>
                                    <div className="mt-1 min-h-5" />
                                  </div>
                                </>
                              ) : (
                                <span className="block px-3 py-3 text-fg-faint">-</span>
                              )}
                            </td>
                          );
                        })}
                      </tr>
                    ),
                  )}
                </tbody>
              </table>
            </div>
          )}
          {data && rows.length === 0 && (
            <p className="py-16 text-center text-sm text-fg-muted">
              No operating companies were found for this portfolio.
            </p>
          )}
        </div>
        <p className="shrink-0 border-t border-neutral-800/40 px-5 py-3 text-xs text-fg-muted">
          Revenue is shown in millions, in each company&apos;s reported
          currency. YoY compares the same reported quarter one year earlier; a
          dash means that comparison has not been stored.
        </p>
      </div>
    </PanelDialog>
  );
}
