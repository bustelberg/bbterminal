# Quality-Filtered Momentum Portfolio — Pipeline Specification

> **Status:** Draft  
> **Last updated:** 2026-03  
> **Owner:** Quantitative Strategy  
> **Version:** 0.1

---

## Overview

This document describes the monthly pipeline for constructing a momentum portfolio drawn exclusively from the firm's quality universe. The strategy is top-down: sectors are ranked first, and individual stocks are only considered within sectors that pass the sector-level threshold. Every position decision is rule-based and explainable.

**Core philosophy:**
- Only invest in companies that already pass the firm's quality filter
- Only invest in sectors the market is currently rewarding
- Only invest in stocks that are individually in an uptrend
- Every buy has a plain-English explanation

**Target portfolio size:** ~25 stocks, equal weighted  
**Rebalance frequency:** Monthly  
**Benchmark:** MTUM (iShares MSCI Momentum ETF)

---

## Data Sources

| Source | Used For |
|---|---|
| Internal DuckDB (quality universe) | Universe definition, financials, price history |
| GuruFocus API — Historical Price | 200MA calculation, momentum returns |
| GuruFocus API — Analyst Estimates | Earnings revision scores |
| GuruFocus API — Real-Time Insider Trades | Insider & politician signal |
| GuruFocus API — Guru Trades | Guru signal |
| GuruFocus API — News Headlines | Media narrative input |
| Claude API | Narrative scoring, report generation |

---

## Pipeline Overview

```
Quality Universe (~200 stocks)
        │
        ▼
PHASE 1 — Sector Rankings
  Ranking A: Price Trend
  Ranking B: Smart Money (insiders / politicians / gurus)
  Ranking C: Media Narrative  ← LLM
        │
  Combined Sector Scorecard
        │
  Threshold cut: sectors scoring < 60 eliminated
        │
        ▼
PHASE 2 — Stock Selection
  Hard filters (200MA, momentum)
  Soft scores (momentum, revisions, insider boost)
        │
  Stock ranking table
        │
        ▼
PHASE 3 — Portfolio Construction
  Concentration rule (max 40% per sector)
  Top ~25 stocks proposed
        │
        ▼
PHASE 4 — Human Review
  Auto-generated report with explanations + flags
  PM approves / vetoes
        │
        ▼
Execute Trades
```

---

## Phase 1 — Sector Rankings

**Runs independently for each ranking, then combined into a single scorecard.**

**Input:** Quality universe stocks (tagged by sector) + price history + GuruFocus data

**Universe note:** Sector scores are computed using only stocks within the quality universe, not the full market. If a sector has fewer than 5 stocks in the universe, supplement with the relevant broad sector ETF return as a sanity check. Do not trade the ETF — use it only as validation.

---

### Ranking A — Price Trend

Answers: *Is the market price action in this sector positive right now?*

**Inputs:**
- Monthly price history per stock (from DuckDB)
- 200-day moving average per stock

**Logic:**
```
For each sector:
  pct_above_200ma     = % of universe stocks in sector trading above 200MA
  return_1m           = equal-weighted 1M return of sector stocks
  return_3m           = equal-weighted 3M return of sector stocks
  return_6m           = equal-weighted 6M return of sector stocks

  raw_score = weighted combination of above
  → normalize to 0-100
```

**Suggested weights within Ranking A:**
- % above 200MA: 40%
- 3M return: 35%
- 1M return: 15%
- 6M return: 10%

**Output:** Price trend score 0–100 per sector

---

### Ranking B — Smart Money

Answers: *Are informed or high-conviction investors buying into this sector?*

**Data source:** GuruFocus Real-Time Insider Trades + Guru Trades API  
**Lookback window:** 60 days

**Three sub-signals:**

**Politicians**
- Count of buy transactions by politicians per sector
- Total $ value of political buys per sector
- Rationale: politicians sometimes trade ahead of policy announcements (defense contracts, regulation changes, subsidies)

**Corporate insiders (CEO / CFO / board)**
- Cluster insider buying = 2 or more insiders at the same company buying in the same 60-day window
- Count of stocks per sector showing cluster insider buys
- Rationale: one insider buy could be anything; a cluster is a signal

**Gurus**
- Count of guru initiations or additions per sector
- Tracked gurus only (e.g. Buffett, Ackman — configurable list)
- Rationale: confirmation from investors with verified long-term track records

**Logic:**
```
For each sector:
  politician_score  = f(buy_count, buy_value) → 0-100
  insider_score     = f(cluster_count) → 0-100
  guru_score        = f(initiation_count) → 0-100

  smart_money_score = weighted average → 0-100
```

**Output:** Smart money score 0–100 per sector

---

### Ranking C — Media Narrative

Answers: *Is there a coherent, current story in the media that explains and supports the sector's move?*

**Data source:** GuruFocus News Headlines API (last 30 days, per sector)  
**Method:** LLM (Claude API) — structured output

**Why LLM here:** A rules-based approach cannot read and interpret text. An LLM processes hundreds of headlines per sector consistently and returns comparable structured scores. This is not predictive ML — it is reading comprehension at scale.

**Prompt logic:**
```
Input:  Last 30 days of headlines for stocks in [sector]
Output: JSON with the following fields:
  {
    "sector": string,
    "narrative_strength": float,   // 0-10
    "key_themes": list[str],       // 3-5 themes
    "sentiment": string,           // positive / neutral / negative / mixed
    "risk_flags": list[str],       // concerning themes if any
    "summary": string              // one sentence plain English
  }
```

**Interpretation:**
- Narrative strength 7–10: strong story, confirmed signal
- Narrative strength 4–6: some coverage, moderate confirmation
- Narrative strength 0–3: sector moving without clear narrative — flag for human review
- Negative sentiment: penalise sector score regardless of price trend

**Output:** Narrative score 0–100 per sector (converted from 0-10 scale)

---

### Combined Sector Scorecard

**Weights (starting point — adjust after backtesting):**

| Ranking | Weight |
|---|---|
| A — Price Trend | 40% |
| B — Smart Money | 35% |
| C — Media Narrative | 25% |

**Output table (example):**

| Sector | Trend | Smart Money | Narrative | Total |
|---|---|---|---|---|
| Defense | 88 | 91 | 85 | **88** |
| Energy | 81 | 74 | 79 | **78** |
| Financials | 74 | 61 | 55 | **64** |
| Technology | 61 | 45 | 71 | **58** |
| Healthcare | 44 | 38 | 41 | **41** |

**Threshold:** Only sectors with Total score ≥ 60 proceed to Phase 2.  
This threshold is configurable. Start at 60, review after 3 months.

---

### Ranking D — Expectations

## Fundamental Support Score

Measures how much of a stock's price move is supported by improving forward fundamentals rather than by multiple expansion alone.

---

### Core idea

A stock can rise for two very different reasons:

1. **Fundamentals improved**  
   Forward EBITDA / EPS estimates moved up.

2. **Investors paid a higher multiple**  
   The market became more optimistic and rerated the stock.

This score separates those two effects.

---

### Formula

Using the actual implemented definitions:

$$
Price\ Return_{30d} = \frac{Price_{now} - Price_{30d\ ago}}{Price_{30d\ ago}}
$$

$$
Multiple\ Change_{30d} = \frac{(EV/EBITDA_{NTM})_{now} - (EV/EBITDA_{NTM})_{30d\ ago}}{(EV/EBITDA_{NTM})_{30d\ ago}}
$$

So the score is defined as:

$$
Fundamental\ Support\ Score = Price\ Return_{30d} - Multiple\ Change_{30d}
$$

Interpretation-wise, this approximates:

$$
Fundamental\ Support\ Score \approx Forward\ Fundamental\ Revision
$$
---

### Interpretation

| Price Return | Multiple Change | Implied Fundamental Support | Signal |
|---|---:|---:|---|
| +20% | +5%  | +15% | ✅ Healthy — fundamentals are supporting most of the move |
| +20% | +20% | 0%   | ⚠️ Neutral — move is mostly rerating, not estimate improvement |
| +20% | -5%  | +25% | ✅ Strongest — estimates improving despite multiple compression |
| +5%  | +20% | -15% | 🔴 Weak — price is running ahead of fundamentals |

---

### Plain English

A **high positive score** means the stock move is backed by higher forward expectations.  
This typically indicates higher-quality momentum.

A **score near zero** means the move is mostly explained by multiple expansion.  
The stock is rising, but analysts are not meaningfully raising forecasts.

A **negative score** means valuation expanded faster than fundamentals improved.  
This often signals sentiment-driven momentum and should be reviewed more carefully.

---

### Practical guidance

- Use consistent lookback windows:
  - 30d
  - 60d
  - 90d  
- Always use the same window for both return and multiple change  
- Prefer EV-based calculations when possible  
- Do not interpret this score in isolation  
- Always combine with:
  - price momentum
  - estimate revisions
  - starting valuation

---

### Sector-level use

At the sector level, take the **median** score across all stocks:

$$
Sector\ Score = median(Fundamental\ Support\ Score_{companies})
$$

The median is preferred because it reduces distortion from outliers.

---

### Suggested interpretation bands

| Score | Meaning |
|---:|---|
| > +0.15 | Strong fundamental support |
| +0.05 to +0.15 | Moderate support |
| -0.05 to +0.05 | Neutral / mixed |
| -0.15 to -0.05 | Weak support |
| < -0.15 | Sentiment-driven — caution |

---

### Important caveats

- This is **not a valuation metric**
- This is **not a standalone signal**
- A high score does not guarantee a good investment
- A negative score can still occur before upward revisions materialize

---

### Key intuition

> Are you buying earnings upgrades, or just paying more for the same story?


## Phase 2 — Stock Selection Within Passing Sectors

**Input:** All quality universe stocks whose sector passed the Phase 1 threshold

---

### Hard Filters (pass/fail — no exceptions)

A stock failing any single filter is eliminated regardless of other scores.

| Filter | Rule | Rationale |
|---|---|---|
| 200MA | Stock price > 200-day moving average | Trend confirmation |
| Absolute momentum | 12-1 month return > 0 | Stock must be going up |
| Relative momentum | 12-1 month return > sector median | Must outperform its own sector |

*12-1 month return = price 12 months ago vs price 1 month ago (last month skipped to avoid short-term reversal effect)*

---

### Soft Scores (surviving stocks ranked 0–100)

| Signal | Weight | Notes |
|---|---|---|
| Individual momentum | 35% | 12-1M return, normalised vs full universe |
| Momentum consistency | 20% | How many of last 6 months were positive |
| Earnings revision score | 25% | Direction + magnitude of analyst estimate changes |
| Insider / political boost | 20% | Stock-level signal from Phase 1 data |

**Insider/political boost rules:**
```
Insider cluster buy (2+ insiders)    → +8 points
Politician buy                       → +5 points
Guru initiation                      → +5 points
Heavy insider selling                → -15 points + ⚠️ flag
```

**Output:** Ranked stock table with composite score and score breakdown

---

## Phase 3 — Portfolio Construction

**Input:** Ranked stock table from Phase 2

**Concentration rule:** Maximum 40% of portfolio in any single sector.  
If the top-ranked sector would exceed this, take the highest-scoring stocks from that sector up to the cap, then fill remaining slots from lower-ranked sectors.

**Portfolio size:** ~25 stocks, equal weighted to start.

**Monthly diff vs previous portfolio:**
- **Additions:** stocks newly entering the top ~25
- **Exits:** stocks that dropped out of the top ~25
- **Holds:** unchanged positions

**Turnover note:** High monthly turnover is a cost. If a stock scores just below the cutoff, consider a buffer rule (e.g. only exit if score drops below 45, not 50) to reduce unnecessary churn.

---

## Phase 4 — Human Review

**Input:** Proposed portfolio from Phase 3 + all underlying scores

**Auto-generated report contains:**

For each proposed addition or exit, one paragraph:
> *"RHM is proposed because Defense ranks 1st across all three sector metrics. The stock sits 27% above its 200-day moving average with 12-month momentum of +94%. Three politicians initiated options positions last month, and media narrative around NATO spending commitments scored 9.1/10 — the strongest of any sector this month."*

**Auto-flags for PM attention:**

| Flag | Condition |
|---|---|
| ⚠️ Insider selling | Heavy insider selling detected at stock level |
| ⚠️ Earnings soon | Earnings announcement within 2 weeks |
| ⚠️ Borderline sector | Sector score 60–65 (just above threshold) |
| ⚠️ Extended | Stock >40% above its 200MA |
| ⚠️ Thin sector | Sector has <5 stocks in universe |

**PM actions:** Review flagged items, veto any obvious problems, approve remainder. Expected time: 20–30 minutes.

---

## Technology Stack

| Component | Tool |
|---|---|
| Data storage | DuckDB |
| Pipeline logic | Python (pandas, numpy) |
| Scheduling | Cron job or Prefect (first Monday of month) |
| Insider/price/news data | GuruFocus API |
| Narrative scoring | Claude API (structured JSON output) |
| Report generation | Claude API or Jinja2 template → PDF/HTML |
| Future: signal optimisation | LightGBM + SHAP (year 2, once sufficient history) |

---

## Build Sequence

| Phase | What | When |
|---|---|---|
| 1 | Sector price trend scoring (Ranking A) | Week 1 |
| 2 | Stock hard filters + momentum scores (Phase 2) | Week 1–2 |
| 3 | Insider/political data pull + Ranking B | Week 3 |
| 4 | LLM narrative scoring (Ranking C) | Week 4 |
| 5 | Portfolio construction + concentration rule | Month 2 |
| 6 | Auto-generated report (Phase 4) | Month 2 |
| 7 | LightGBM signal weight optimisation | Month 12+ |

---

## Open Questions / Decisions Pending

- [ ] Confirm sector taxonomy to use (GICS? custom?)
- [ ] Define tracked guru list
- [ ] Confirm minimum stocks per sector threshold (currently 5)
- [ ] Confirm sector concentration cap (currently 40%)
- [ ] Confirm portfolio size (currently ~25)
- [ ] Confirm rebalance day (currently first Monday of month)
- [ ] Decide on turnover buffer rule
- [ ] Backtest Ranking A/B/C weights before going live

---

## Changelog

| Version | Date | Notes |
|---|---|---|
| 0.1 | 2026-03 | Initial draft |