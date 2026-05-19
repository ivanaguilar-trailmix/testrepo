# Prestige Feature Analysis — Context

## Purpose

Analyses the Prestige feature introduced in Timed Album liveops events. Covers engagement (how many users reached prestige and completed events), revenue attribution for completers, hangover effects across subsequent events, and gem economy behaviour for completers vs. non-completers.

---

## File Structure

```
prestige.ipynb                 — main notebook
prestige.html                  — exported HTML report
sql/prestige.sql               — activity, progression, and revenue data per user per day
sql/economyflow.sql            — gem, coin, and energy currency flows per user per day
data/prestige_data.pkl         — cached output of prestige.sql
data/economyflow.pkl           — cached output of economyflow.sql
common_lib/sql.py              — BigQueryConnector (get, print_cost_estimate)
common_lib/export.py           — export_notebook_html
```

---

## Events Covered

| Event name | `liveops_event_definition_id` | Start date |
|---|---|---|
| JoesTastyTravels | TimedAlbum-JoesTastyTravels | 2025-11-03 |
| WinterTales | TimedAlbum-WinterTales | 2025-12-15 |
| AppletonLove | TimedAlbum-AppletonLove | 2026-01-25 |
| Diorama | TimedAlbum-Diorama | 2026-03-07 |
| ThroughTheAges | TimedAlbum-ThroughTheAges | 2026-04-17 |

Prestige (tier 2+) was introduced in **Diorama**. ThroughTheAges is the second event with the prestige mechanic.

---

## Key Definitions

| Term | Definition |
|---|---|
| **Completer** | User who reached `n_sets_completed == 12` in a given Timed Album event |
| **Prestiger** | User whose `max_tier_reached >= 2` in Diorama (`users_reached_prestige` variable) |
| `completer_any_flag` | 1 if user completed any Timed Album; used in economy analysis |
| `days_since_event_start` | Days from the user's personal event start date; capped at 39 in the notebook |

> **Note:** `users_reached_prestige` is defined as `max_tier_reached == 2` (not `>= 2`), so users who went to tier 3+ are tracked separately in the tier distribution analysis but are not included in the prestige revenue/ARPDAU analysis. Keep this in mind if extending the prestige revenue analysis.

---

## Key Parameters

| Parameter | Where set | Default | Notes |
|---|---|---|---|
| `start_date` | SQL query | `'2025-11-01'` | Covers all five events |
| `refresh_flag` | Activity data cell | `False` | Set `True` to re-query BQ and overwrite pickle |
| `refresh_flag` | Economy data cell | `False` | Same — economy pickle is separate |
| `days_since_event_start` cap | Fix some data section | 39 | Values > 39 are set to `None` |

---

## BQ Tables Used

### `prestige.sql`

| Table | Usage |
|---|---|
| `merger_prod_fact.fact_dt_user_activity` | Active users per day |
| `merger_prod_dimensions.dim_user_install_device` | Platform info |
| `merger_prod_dimensions.dim_user_install_session` | Install session |
| `merger_prod_fact.fact_dsi_user_progression_cumulative` | Player level (`max_level`) |
| `merger_prod_fact.fact_dse_user_timed_album_progression_cumulative` | Sets completed, tier reached, sticker/dupe metrics |
| `merger_prod_fact.fact_dt_user_iap_revenue` | IAP revenue per user per day |
| `merger_prod_fact.fact_dt_user_ad_revenue` | Ad revenue per user per day |
| `merger_prod_dimensions.dim_users_to_exclude` | Excluded users filter |

### `economyflow.sql`

| Table | Usage |
|---|---|
| `merger_prod_fact.fact_dt_user_reason_detail_gem_flows` | Gem inflows/outflows by reason |
| `merger_prod_fact.fact_dt_user_reason_detail_coin_flows` | Coin inflows/outflows by reason |
| `merger_prod_fact.fact_dt_user_reason_detail_energy_flows` | Energy inflows/outflows by reason |
| `merger_prod_fact.fact_dsi_user_economy_balance_snapshot` | Daily balance start/end per currency |
| `merger_prod_dimensions.dim_users_to_exclude` | Excluded users filter |

---

## Key Columns

### `data` / `data_extended` (from `prestige.sql`)

| Column | Description |
|---|---|
| `user_id`, `dt` | User + date grain |
| `max_level` | Player level on that day |
| `liveops_event_definition_id` | Event name (cleaned — `S5_ThroughTheAges` merged into `ThroughTheAges`) |
| `liveops_iteration_counter` | Which run of the event for this user (1 = first time) |
| `liveops_user_start_dt` | Date this user started the event |
| `days_since_event_start` | Days since user's personal event start (capped at 39) |
| `n_sets_completed` | Cumulative sets completed in the event |
| `max_tier_reached` | Highest prestige tier reached (1 = no prestige, 2+ = prestiged) |
| `n_stickerpack_opened`, `n_stickers_collected`, etc. | Sticker engagement metrics (cumulative) |
| `dupepoints_inflow/outflow/balance_*` | Dupe points economy within the event |
| `gross_usd_iap_revenue`, `gross_usd_ad_revenue` | Revenue on that day (NaN = no revenue, not 0) |
| `user_id_d1/d7/d14/d28` | User ID if the player was active D+1/7/14/28 (null otherwise) — used for return rate |
| `reached_prestige_user_id` | Non-null if user ever reached prestige in Diorama |
| `completer_user_id` | Non-null on days when `n_sets_completed == 12` |

### `data_econ` (from `economyflow.sql`)

| Column | Description |
|---|---|
| `user_id`, `dt`, `currency` | User + date + currency grain (gems/coins/energy) |
| `balance_start`, `balance_end` | Balance at start/end of day |
| `n_economy_inflow` | Total inflow (free + paid + ads) for the day |
| `n_economy_outflow` | Total outflow (negative values in source, stored as positive) |
| `completer_any_flag` | Added in notebook: 1 if user completed any Timed Album |

---

## Key Findings (as of 2026-04-28 data pull)

- **8,327 users** (5.2% of Diorama participants) reached Prestige; a further 1,759 went to tier 3+
- Completers are **6.3% of Diorama users** but generate **15.4% of IAP revenue** (~2.4× their user share)
- IAP share of completers has grown consistently across events: 8.4% → 9.1% → 14.7% → **15.4%**
- D1 return rate for Diorama completers during their own event: **98.9%**; D28: **97.1%**
- Completers hold **~3× more gems** than the average player (891 → 1,422 gems over time vs. 308 → 431 overall)
- Gem balance growth for completers is gradual and steady — no evidence of sudden inflation

---

## Notebook Structure

```
Main questions          — research questions + data-backed answers
Aux functions           — vline config + add_vlines_to_figure helper
Get data                — BQ queries + pickle cache
  activity              — prestige.sql
  Economy flows         — economyflow.sql
  Fix some data         — merge S5_ThroughTheAges, fill NaN events, cap DSE at 39
Health check and data setup  — build data_extended, flag completers + prestigers
  Daily active users check   — sanity check on coverage and event overlap
Completers              — users who completed 12 sets
  Per Event             — aggregate counts and revenue shares
    Users               — bar charts of completer counts and rates
    Revenue             — bar charts of IAP/ad revenue share
  Daily                 — time-series by calendar date and days since event start
    Activity            — daily completer counts and rates
    Revenue             — daily revenue for completers
    Hangover            — completer cohorts tracked across all subsequent events
      Return rate       — D1/D7/D14/D28 return rates per cohort per event
      Revenue           — daily revenue per cohort
      ARPDAU            — daily ARPDAU per cohort
Prestigers              — users who reached tier 2+ in Diorama
  Engagement            — tier distribution and daily tier progression
  Revenue               — ARPDAU and IAP revenue over time and by DSE
Economy                 — currency flow analysis (all three currencies)
  Prepare data          — add completer_any_flag
  Gems                  — deep-dive on gem economy
    Users               — daily gem users by completer flag
    Ending balances     — avg + percentile balances; user distribution
    Net flow            — daily net gem flow
    Inflow vs Outflow   — avg inflows vs outflows side-by-side
Economy by completer cohort  — gem metrics per completer cohort across events
  Completers for each event  — avg balance, inflows, outflows per cohort + completer vs non-completer comparison
Suggested additional analyses — open questions and proposed next steps
```

---

## common_lib

- `BigQueryConnector.get(query, is_path, query_parameters)` — runs SQL, returns DataFrame; `query_parameters` dict values substituted as `{key}` placeholders in the SQL
- `BigQueryConnector.print_cost_estimate(...)` — dry-run cost estimate, call before `get`
- `export_notebook_html(notebook_path, output_path)` — exports executed notebook to a self-contained HTML with inline Plotly, sidebar TOC, and collapsible cells

**Cell visibility markers (first line of cell):**

| Cell type | Marker | Effect in exported HTML |
|---|---|---|
| Code | `# show` | Show code input by default (inputs are hidden by default) |
| Code | `# hide-output` | Hide output by default (outputs are shown by default) |
| Markdown | `<!-- hide -->` | Hide cell content by default with a "▶ Show note" toggle; headings excluded from TOC |
| Markdown | `<!-- hide-toc -->` | Cell visible, but headings excluded from the TOC sidebar |

---

## Gotchas

- `S5_ThroughTheAges` is a duplicate ID for ThroughTheAges — merged in "Fix some data"; always use the cleaned `data` or `data_extended`, not the raw query output.
- Revenue columns (`gross_usd_iap_revenue`, `gross_usd_ad_revenue`) are **NaN on days with no revenue**, not 0. Use `sum()` for aggregation — `mean()` will give inflated per-user figures.
- `users_reached_prestige` uses `max_tier_reached == 2` (exactly tier 2), so tier 3+ users are **not** included in prestige revenue/ARPDAU calculations. Total prestige-reached users = 8,327 (tier 2) + 1,759 (tier 3+) = 10,086.
- ThroughTheAges shows 0 completers in the per-event summary — the event started 2026-04-17 and the data pull was through 2026-04-28 (only 11 days in), not enough time for completers to appear.
- The gap between Diorama completers (9,390) and Prestigers (8,327) is ~1,063 users. Prestige likely requires an explicit action after completing all sets, or has a timing window separate from set completion.
- `days_since_event_start` uses the user's **personal** start date (`liveops_user_start_dt`), not the event's global start date. Users who start late will have lower DSE values than the calendar would suggest.
