# AB Test Seed Selection — Context

## Purpose

Finds the best random seed (salt) for AB test user bucketing. Runs a simulated AA test over 30 candidate salts on historical data and picks the one that produces the most balanced 50/50 split by ARPDAU.

---

## File Structure

```
abtestseed.ipynb          — main notebook
sql/abtestseed.sql        — Phase 1: seed evaluation across 30 random salts
sql/abtestseedmetrics.sql — Phase 2: daily metric breakdown for the chosen seed
data/abtestseed.pkl       — cached output of abtestseed.sql
data/abtestseedmetrics.pkl — cached output of abtestseedmetrics.sql
common_lib/sql.py         — BigQueryConnector (get, print_cost_estimate)
common_lib/export.py      — export_notebook_html
```

---

## Bucketing Algorithm

Both SQL files use the same FNV-1a 32-bit hash bucketing function:

```
bucket = fnv32a(salt + userId) % 100
```

Bucket config is base64-encoded JSON: `[[{"min":0,"max":49}],[{"min":50,"max":99}]]` — a strict 50/50 split. Variant 0 = buckets 0–49, Variant 1 = buckets 50–99.

The `abTestWeightedBucketsBase64` column in the output is the encoded config used, included so the exact split can be reproduced in the live AB test.

---

## Phase 1 — Seed Evaluation (`abtestseed.sql`)

- `abteststartdate` defaults to yesterday; `daterange_start` = startdate − `lookback_days`
- Generates 30 random 9-character hex salts via `generateSeeds()`
- Cross-joins all active users against all 30 salts, assigns each user a bucket per salt
- Aggregates per (salt, bucket): DAU, DPU_0 (payers), DPU_10 (payers >$10), iapnetrevenue, adrevenue, ARPDAU, AD_ARPDAU
- Computes `ARPDAU_DIFF` and `AD_ARPDAU_DIFF` as relative lift of bucket 1 vs bucket 0
- **Selection criterion**: pick the salt with minimum `ABS_ARPDAU_DIFF` for bucket 1

### BQ tables used
| Table | Usage |
|---|---|
| `merger_prod_fact.fact_dt_user_activity` | Active users per day (`active=1`) |
| `merger_prod_dimensions.dim_user_install_device` | Platform filter (excludes nulls) |
| `merger_prod_fact.fact_dt_user_iap_revenue` | IAP net revenue per user per day |
| `merger_prod_fact.fact_dt_user_ad_revenue` | Ad revenue per user per day |

---

## Phase 2 — Seed Validation (`abtestseedmetrics.sql`)

- Takes `{seed}` as a parameter (the selected salt from Phase 1)
- Assigns every active user a variant (0 or 1) using the same FNV-1a bucketing
- Aggregates daily: users, iapnetrevenue, adrevenue, arpdau, adrpdau per variant
- Computes daily `_diff` columns as **(control − treatment) / control** — so negative = treatment higher

### Output columns
| Column | Meaning |
|---|---|
| `arpdau_diff` | Relative diff of IAP ARPDAU, variant 0 vs 1 |
| `adarpdau_diff` | Relative diff of ad ARPDAU, variant 0 vs 1 |
| `iapnetrevenue_diff` | Relative diff of total IAP revenue |
| `adrevenue_diff` | Relative diff of total ad revenue |

Note: diff = 0 for variant 1 rows (NaN in pandas after pivot); only variant 0 rows carry the diff value.

---

## Key Parameters

| Parameter | Where set | Default | Notes |
|---|---|---|---|
| `lookback_days` | Notebook cell 2 & 6 | 28 | Days before yesterday for both phases |
| `force_refresh` | Notebook cell 3 | False | Set True to bypass pickle cache and re-query |
| `force_refresh` | Notebook cell 7 | True | Phase 2 always re-queries by default |
| `seed` | Notebook cell 6 | auto-selected | Overridable with a hardcoded salt string |

---

## common_lib

- `BigQueryConnector.get(query, is_path, query_parameters)` — runs SQL, returns DataFrame; `query_parameters` dict values are substituted as `{key}` placeholders in the SQL
- `BigQueryConnector.print_cost_estimate(...)` — dry-run cost estimate, call before `get`
- `export_notebook_html(notebook_path, output_path)` — exports executed notebook to HTML

---

## Gotchas

- The SQL uses `{{` / `}}` to escape literal braces inside f-string-style parameter substitution (the JS UDFs contain lots of braces)
- Phase 2 `diff` columns are computed with `lead()` over `(partition by dt order by dt, variant)` — so variant 1 rows always get NaN diff in the Python output; only variant 0 rows have meaningful diff values
- `seeds` are regenerated fresh on every Phase 1 run — the pickle cache locks in one set of seeds; set `force_refresh=True` to get new seeds
- The `abTestWeightedBucketsBase64` output column should be copied verbatim into the live AB test config to guarantee the same split
