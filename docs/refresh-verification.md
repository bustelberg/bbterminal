# Portfolio refresh verification — 9 November 2026

Manual, authenticated, read-only source checks from the local workstation. No
portfolio changes or database writes were made. These measure source retrieval,
not deployed UI latency, database persistence or popup transport.

## Leonteq

The baseline uses the existing portal/CSV readers with an authenticated browser
and a genuinely reloaded page. The fast path requests the same authenticated AMC
API endpoints, including the source's sort parameter and cache revalidation.

| Read | Portal baseline | Fast-path samples | Approximate improvement using median |
| --- | ---: | --- | ---: |
| Portfolio list, 13 rows | 2.844 s | 0.234 / 0.135 / 0.130 s | 21× |
| Holdings, 21 positions | 5.340 s | 0.440 / 0.458 / 0.471 s | 12× |
| Monthly returns, 12 observations | 3.505 s | 0.154 / 0.129 / 0.120 s | 27× |

Cold authentication still took about 7–9 seconds. These measurements do not
establish a universal 100× improvement or production latency.

The live parity checks confirmed:

- All 13 catalogue rows matched sponsor, start date, value, AuM, inception return
  and YTD return. Source names are trimmed like the portal.
- The sampled composition had the same 21 holding identities as the CSV; weights,
  base-currency values, units and prices agreed within 0.01 display rounding.
- The sampled monthly observations and yearly totals matched the original Details table.

The check also found that fragment-only navigation could reuse the previous
Angular page: its misleading 0.041-second result was discarded. The fallback now
forces a page reload; the fast path always makes a new authenticated API request.

## AIRS

| Read | Observed source time | Result |
| --- | ---: | --- |
| Catalogue, cold | 7.436 s | 98 models |
| Catalogue, warm | 1.365 s | 98 models |
| Fixed composition | 3.683 s | 21 positions |
| Dynamic holdings report | 0.227 s | 37 positions |

Dynamic inspection now requests the holdings report alone rather than the five
reports in a complete account scan. Fixed report export remains a slower source
operation. The daily full-account scan retains all of its reports.

## Regression checks

Unit checks cover fresh requests without page navigation, partial-response
rejection, composition reconciliation, zero/missing returns, nested source
precision, session reuse/expiry, cancellation and result-load failure popups.
