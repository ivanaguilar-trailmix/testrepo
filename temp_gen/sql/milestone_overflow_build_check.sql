DECLARE window_after_event_ends INT64 DEFAULT 14;
DECLARE target_liveops_event_definition_id STRING DEFAULT 'GeneratorEvent-GardenGlory';
DECLARE target_liveops_iteration_id STRING DEFAULT '202608171000';
DECLARE ladder_length INT64 DEFAULT 16;

-- Investigates whether players completing more than the event's configured 16 milestones
-- (LPD-261 - confirmed real in rp_LOpsMilestoneComplete, not a dbt/Omni artifact) are
-- disproportionately on an older game build. Compares build version at the moment each player
-- crossed the ladder length (their first over-the-cap completion) against a baseline of players
-- who stopped exactly at the ladder cap, for a like-for-like comparison.
--
-- dimchange_user_build is a type-2 SCD, one row per build change per user: user_id,
-- build_version, effective_ts (validity start), expiry_ts (validity end, open-ended for the
-- current build). Resolving "what build was this player on at time T" needs
-- effective_ts <= T AND (expiry_ts > T OR expiry_ts IS NULL) below - confirm expiry_ts is
-- actually NULL (not some sentinel like '9999-12-31') for open-ended rows before trusting this.

WITH latest_milestone_progression AS (
        SELECT
              user_id
            , liveops_event_start_ts
            , liveops_event_end_ts
            , milestone
            , event_ts
        FROM trailmixgames-game-1.merger_prod_raw_processing.rp_LOpsMilestoneComplete
        WHERE context IN ('goal', 'goalCompleted')
        AND liveops_event_definition_id = target_liveops_event_definition_id
        AND liveops_iteration_id = target_liveops_iteration_id
        AND liveops_event_type_ID IS NOT NULL
        AND liveops_event_type_ID NOT LIKE 'Unknown%'
        AND DATE(event_ts) >= '2026-08-17'
)

, int_ts_user_liveops_milestone_progression AS (
    SELECT
          user_id
        , milestone
        , MIN(event_ts) AS min_ts
    FROM latest_milestone_progression
    WHERE event_ts BETWEEN CAST(liveops_event_start_ts AS DATETIME) AND DATE_ADD(CAST(liveops_event_end_ts AS DATETIME), INTERVAL window_after_event_ends day)
    GROUP BY ALL
)

-- Overflow cohort: players who went past the configured ladder, paired with the timestamp of
-- their first over-the-cap completion (milestone = ladder_length + 1) - the moment we want the
-- build version for.
, overflow_cohort AS (
    SELECT
          user_id
        , MIN(min_ts) AS cohort_defining_ts
        , MAX(milestone) AS furthest_milestone
        , 'overflow' AS cohort
    FROM int_ts_user_liveops_milestone_progression
    WHERE milestone = ladder_length + 1
    GROUP BY user_id
)

-- Baseline cohort: players who reached exactly the ladder cap and no further, paired with the
-- timestamp of that final completion, for a like-for-like build-version comparison.
, baseline_cohort AS (
    SELECT
          user_id
        , MAX(min_ts) AS cohort_defining_ts
        , MAX(milestone) AS furthest_milestone
        , 'baseline_capped_at_ladder' AS cohort
    FROM int_ts_user_liveops_milestone_progression
    GROUP BY user_id
    HAVING MAX(milestone) = ladder_length
)

, users_to_exclude AS (
    SELECT DISTINCT
        user_id
    FROM trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude
)

, cohorts AS (
    SELECT * FROM overflow_cohort
    UNION ALL
    SELECT * FROM baseline_cohort
)

SELECT
      c.user_id
    , c.cohort
    , c.furthest_milestone
    , c.cohort_defining_ts
    , b.build_version
FROM cohorts c
LEFT JOIN users_to_exclude u ON c.user_id = u.user_id
LEFT JOIN trailmixgames-game-1.merger_prod_dimensions.dimchange_user_build b
    ON c.user_id = b.user_id
    AND c.cohort_defining_ts >= b.effective_ts
    AND (c.cohort_defining_ts < b.expiry_ts OR b.expiry_ts IS NULL)
WHERE u.user_id IS NULL
