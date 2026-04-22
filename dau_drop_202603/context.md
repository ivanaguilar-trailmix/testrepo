---
name: DAU Drop Analysis Mar 2026
description: Full context for the dau_drop_202603 investigation notebook — structure, periods, hypotheses, and all design decisions
type: project
---

## Location
`/Users/ivanaguilar/Desktop/DataStuff/gitrepos/testrepo/dau_drop_202603/exploration.ipynb`

## Goal
Investigate an acceleration of DAU decline on a mobile game observed in early 2026. Identify which install cohorts are driving the drop and quantify the magnitude.

## Analysis Periods (all 21 days, Monday-aligned, as of Mar 2026)
| Period | Start | End |
|---|---|---|
| Recent | 2026-02-23 (Mon) | 2026-03-15 (Sun) |
| Baseline YoY | 2025-04-21 (Mon) | 2025-05-11 (Sun) |
| Baseline Oct20 | 2025-10-20 (Mon) | 2025-11-09 (Sun) |

Data covers 2025-02-09 to 2026-03-15. `lookback_days = 440` in the query to reach Jan 1, 2025.

## Notebook Structure (cell IDs)
- `nn3w9sg3uye` — Title markdown
- `d38c54fd` — Imports
- `oi849k1lo7l` — §1 Data Pull markdown
- `fbd64630` — Query setup + `bqc.print_cost_estimate()`, `lookback_days=440`
- `f19b8a54` — Conditional data pull (pickle cache, `force_refresh` flag)
- `368285b4` — `data` display
- `703f56f2` — Player level data pull markdown
- `6fb1dad0` — player_level.sql cost estimate
- `d5af86b0` — player_level data pull
- `f196a8a4` — `pl_data` display
- `nr58jqolz5p` — §2 Feature Engineering markdown
- `775fac72` — `days_since_install_bin` cut bins
- `2db6fe0a` — `bin_install_dt()` function + apply
- `2fb0438tzl1` — §3 Aggregations markdown
- `9b59fb9a` — `cohort_data` groupby
- `70qgle2yvsw` — §3.2 New installs markdown
- `8g02ollleoi` — `new_installs_by_dt` (days_since_install==0)
- `thcpywsgr9` — §3.3 Retention markdown
- `7c7j99qnowj` — D1/D7/D28 retention → `retention_df`
- `jnjehbbcws` — §4 Timeline Charts markdown
- `c87c7001` — Line chart (cohorts + installs y2 + retention y3 + ref lines)
- `de8521ca` — Area chart (same overlays)
- `5e4986a2` — Empty cell
- `yitz06424n` — §5 markdown with period table
- `kldvr23u6c` — `analyze_dau_drop()` function definition
- `h36ex088ev6` — YoY comparison call
- `1e7dy4p4mx` — Oct20 baseline call
- `aa893844` — §6 Player Level analysis markdown
- `837c7276`, `5103e8c1`, `347a8625`, `e15c5393` — player level analysis cells

## Cohort Binning (`bin_install_dt`)
- Before 2021, 2021, 2022, 2023, 2024 → yearly label
- 2025 → quarterly (2025-Q1, Q2, Q3, Q4)
- 2026 → weekly (2026-W01, W02, …)

## Key Functions / Data
- `cohort_data` — groupby(dt, install_dt_bin) → users (nunique)
- `new_installs_by_dt` — days_since_install==0 proxy for install date
- `retention_df` — D1/D7/D28 keyed to install_dt (shifted by d days)
- `total_by_dt` — daily total DAU (all cohorts summed)

## `analyze_dau_drop()` Design
Three charts per comparison:
1. **fig1** — Bar: absolute delta per cohort, customdata=(baseline_avg, recent_avg, pct_change)
2. **fig2** — Bar: % share of drop (declining cohorts only), customdata=(delta, baseline_avg, recent_avg)
3. **fig3** — Dual y-axis line+markers overlay (day-indexed 0–20):
   - y1 steelblue = baseline, y2 crimson = recent
   - Regression trend lines (dashed, `hoverinfo='skip'`)
   - **Matched y-axis ranges**: both axes share the same span = `max(baseline_fluct, recent_fluct) * 1.4`, each centered on its own series mean — ensures slopes are visually comparable without inflating scale by the absolute gap between periods
   - customdata=(date_str, dow) for hover: `Day X / Date / DayOfWeek / DAU`

## Reference Lines (both §4 timeline charts)
```python
('2025-04-21', 'Baseline YoY start'), ('2025-05-11', 'Baseline YoY end'),
('2025-10-20', 'Baseline Oct20 start'), ('2025-11-09', 'Baseline Oct20 end'),
('2026-02-23', 'Recent start'), ('2026-03-15', 'Recent end'),
```
Red, width=1.

## `common_lib/sql.py` Changes Made
- BQ pricing corrected to **$6.25/TB** (was $0.50/TB)
- Fixed `/ 10**-12` → `* 10**-12` in TB warning in `get()`
- Added `_format_bytes(bytes_processed)` static method (auto-scales KB/MB/GB/TB)
- Added `print_cost_estimate(query, is_path, query_parameters)` — wraps `check_query_cost`, prints BQ-style summary, returns full dict

## Key Hypotheses (from analysis as of Mar 2026)
1. Rate of decline not significantly faster — marginal acceleration only (regression slopes broadly similar)
2. Thin 2025+ cohorts + UA spend cut are the primary structural gap driver
3. Pre-2025 cohorts stabilised Dec 2025–Feb 2026 and have since resumed prior churn pace
4. Retention quality of 2025 cohorts may be weaker (D28 line trending down through 2025)
5. Install pipeline depletion from spend cut has lagged DAU effect landing now
6. Possible content/progression ceiling driving simultaneous churn across cohorts (check §6 player level)
7. Seasonal baseline mismatch — Oct 2025 comparison is cleaner signal than YoY Apr/May

## Environment
- Python venv: `/Users/ivanaguilar/Desktop/DataStuff/gitrepos/testrepo/.trailmix_basic_venv`
- Do NOT run the notebook programmatically — user runs it manually
- SQL files: `./sql/dsi_activity.sql`, `./sql/player_level.sql`
- Data cached at `./data/dsi_activity.pkl`, `./data/player_level.pkl`
