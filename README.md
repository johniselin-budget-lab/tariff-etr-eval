# Tariff ETR Evaluation

Comparing actual vs. statutory effective tariff rates during the 2025--2026 US tariff escalation.

## Overview

This project evaluates the gap between **statutory** tariff rates (what the Harmonized Tariff Schedule says importers should pay) and **actual** collection rates (customs duties actually collected as a share of import value). The gap reflects a mix of collection timing, trade agreement utilization, avoidance, stockpiling, and evasion.

## Data Sources

| Source | Repo | What |
|--------|------|------|
| `rate_timeseries.rds` | `tariff-rate-tracker` | HTS10 × country statutory rates by revision |
| `daily_overall.csv` | `tariff-rate-tracker` | Daily import-weighted statutory ETR |
| `daily_by_authority.csv` | `tariff-rate-tracker` | Daily ETR decomposed by tariff authority |
| `daily_by_country.csv` | `tariff-rate-tracker` | Daily ETR by trading partner |
| `tariff_revenue.csv` | `tariff-impact-tracker` | Monthly actual ETR (customs duties / imports) |

Both sibling repos must be present at the same directory level (e.g., `Documents/GitHub/tariff-rate-tracker` and `Documents/GitHub/tariff-impact-tracker`).

## Report Contents

1. **Actual vs. Statutory ETR** (Figure 1): Daily statutory line + monthly actual scatter
2. **Statutory-Actual Gap** (Figure 2): Monthly gap in percentage points
3. **Country-Level Decomposition** (Figure 3): Statutory ETR by major trading partner
4. **Stockpiling Analysis** (Figure 4): Tariff increases vs. import timing by HS chapter
5. **Gap Decomposition Framework** (Figure 5): Illustrative attribution to avoidance channels

## Usage

```r
source("run_all.R")
```

Or render the report directly:

```r
rmarkdown::render("R/etr_eval_report.Rmd", output_dir = "output")
```

## Methodology

**Monthly collapsing**: Statutory rates are collapsed from daily to monthly using first-of-month snapshots, preserving the step-function nature of tariff policy.

**Gap decomposition**: The statutory-actual gap is decomposed into timing lags, USMCA utilization, stockpiling, other preferences, and evasion. See the report for details on each channel.
