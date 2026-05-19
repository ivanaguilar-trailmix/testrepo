# Player Level & Game Day — Cohort Performance Analysis

**Date:** 2026-05-05
**Data:** Monthly cohorts, 2024-04-01 to 2026-04-01 (~58.7M rows)
**Benchmark:** Median across all cohorts at each `days_since_install` (minimum 8 cohorts reporting)

---

## TL;DR

There are two distinct underperformance patterns in the data, involving different cohort groups and different parts of the player journey.

---

## Pattern 1 — Deep, persistent underperformance: Q1 2025 cohorts (Feb / Mar / Apr 2025)

These are the worst-performing cohorts across the entire dataset, and the problem compounds over time rather than self-correcting:

| Cohort | Avg level deviation | Avg gameday deviation |
|---|---|---|
| 2025-Mar | -5.2% | -6.8% |
| 2025-Apr | -3.9% | -6.2% |
| 2025-Feb | -3.5% | -4.8% |

They are below median in every segment of the journey — early, mid, late, and long-term (90+ days). The underperformance is worst in the mid/late segments (d7–d90), which suggests it is not a tutorial/onboarding issue but rather a mid-game engagement problem.

### Where the gap accelerates most sharply

- Around **d143–d150**: game day ~49–51, player level ~50 — a first inflection point where the gap jumps
- Around **d185–d210**: game day ~58–65, player level ~55–60 — this is where the gap widens most violently (up to -14% on game day for Mar 2025)

These are likely **content walls** — level 50 and ~level 57–60 are candidate checkpoints where these cohorts disproportionately stalled or churned. The fact that it hits both metrics (level and game day in tandem) makes it more credible as a structural barrier rather than noise.

### Player level at key milestones — Q1 2025 vs median

| Day | Median (all cohorts) | Feb 2025 | Mar 2025 | Apr 2025 |
|---|---|---|---|---|
| d7 | 10.5 | — | — | -3.2% |
| d14 | 13.7 | — | — | -5.1% |
| d30 | 20.0 | -5.1% | -5.8% | -4.2% |
| d60 | 29.4 | -4.8% | -5.8% | -4.2% |
| d90 | 37.0 | -3.2% | -6.0% | -3.7% |

### Game day at key milestones — Q1 2025 vs median

| Day | Median (all cohorts) | Feb 2025 | Mar 2025 | Apr 2025 |
|---|---|---|---|---|
| d7 | 7.4 | — | — | -4.2% |
| d14 | 10.3 | — | — | -5.4% |
| d30 | 16.3 | -6.0% | -7.1% | -5.1% |
| d60 | 27.6 | — | — | — |
| d90 | 37.4 | -4.5% | -7.5% | -7.1% |

---

## Pattern 2 — Early onboarding deficit, then recovery: Oct / Nov 2025 cohorts

These cohorts start well below median on day 0 and normalize by day ~12–14:

| Cohort | D0 level deficit | D7 level deficit | D14 level deficit |
|---|---|---|---|
| 2025-Sep | -4.9% | -1.0% | +0.2% |
| 2025-Oct | -10.3% | -3.6% | -1.9% |
| 2025-Nov | -10.1% | -3.6% | -2.0% |

The deficit is steepest on D0–D2 (median level at D0 is 3.83; Oct/Nov cohorts reach only 3.44). This points to something specific affecting first-session quality — tutorial friction, early level pacing, or a UA source shift bringing in lower-intent players in that window.

By d30 they are back at median, and by d60 they are essentially indistinguishable from normal cohorts. This is an acquisition/onboarding issue, not a content problem.

---

## Recent cohorts (May 2025 onwards) — overall picture

The picture is positive outside of the Oct/Nov onboarding dip.

### Player level vs median at d30

| Cohort | Level at d30 | vs median |
|---|---|---|
| 2025-May | 20.7 | +3.4% |
| 2025-Jun | 21.0 | +4.8% |
| 2025-Jul | 20.6 | +3.2% |
| 2025-Aug | 20.5 | +2.7% |
| 2025-Sep | 20.1 | +0.6% |
| 2025-Oct | 20.1 | +0.4% |
| 2025-Nov | 19.8 | -0.9% |
| 2025-Dec | 20.8 | +4.1% |
| 2026-Jan | 21.5 | +7.5% |
| 2026-Feb | 21.6 | +8.2% |
| 2026-Mar | 21.8 | +9.2% |

**Dec 2025 / Jan–Mar 2026** are the strongest cohorts in the entire dataset. Jan/Feb 2026 are consistently +7–10% above median at d30, d60, and d90. If this trend holds it represents a meaningful improvement in the player experience.

**2025-Sep at d90** sits at -3.3% on level and -4.3% on game day — worth monitoring, but only just crosses the significance threshold.

---

## Summary of action items implied by the data

| Finding | Likely root cause to investigate | Cohorts affected |
|---|---|---|
| Persistent -5–7% gap through entire journey | Mid-game content wall around level 50 and level 57–60 / game day 50 and 60–65 | Feb / Mar / Apr 2025 |
| -10% D0 deficit that self-corrects by D14 | First-session experience or UA source shift | Oct / Nov 2025 |
| Strong outperformance in 2026 cohorts | Likely positive product change — worth identifying what shipped | Jan–Mar 2026 |
