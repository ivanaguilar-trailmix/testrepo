DECLARE window_after_event_ends INT64 DEFAULT 14;
DECLARE target_liveops_event_definition_id STRING DEFAULT 'GeneratorEvent-GardenGlory';
DECLARE target_liveops_iteration_id STRING DEFAULT '202608171000';

-- Point-milestone ladder for GeneratorEvent-GardenGlory, built to mirror Omni's
-- fact_dt_user_liveops_ms_event_progression / "Milestone Distribution" tile, but scoped to
-- context != 'itemPath' explicitly (see tempgen.sql's item-path query for the other half of
-- this event's two ladders). Built to independently test whether the raw event data itself
-- caps at the event's configured 16 milestones, or whether the >16 tail seen in Omni's fact
-- already exists upstream in rp_LOpsMilestoneComplete.
WITH latest_milestone_progression AS (
        SELECT
              user_id
            , liveops_event_start_ts
            , liveops_event_end_ts
            -- same 1-based convention as item_level in tempgen.sql - do not offset again
            , milestone
            , event_ts
        FROM trailmixgames-game-1.merger_prod_raw_processing.rp_LOpsMilestoneComplete
        -- the point-milestone ladder; itemPath is the separate item-path ladder (see tempgen.sql)
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
        , liveops_event_start_ts
        , milestone
        , MIN(event_ts) AS min_ts
    FROM latest_milestone_progression
    -- keep only rows where the completion happened between the event starting and within 2 weeks of it ending
    WHERE event_ts BETWEEN CAST(liveops_event_start_ts AS DATETIME) AND DATE_ADD(CAST(liveops_event_end_ts AS DATETIME), INTERVAL window_after_event_ends day)
    GROUP BY ALL
)

, milestone_progression_by_user_day AS (
    SELECT
          user_id
        , DATE(min_ts) AS dt
        , TIMESTAMP_DIFF(min_ts, CAST(liveops_event_start_ts AS DATETIME), DAY) AS days_since_event_start
        , COUNT(DISTINCT milestone) AS n_milestones_completed
        , MAX(milestone) AS max_milestone_completed
        , MIN(min_ts) AS first_completion_ts
    FROM int_ts_user_liveops_milestone_progression
    GROUP BY ALL
)

-- Full funnel population - the base_view Omni's topic always_left-joins milestone completions
-- onto (same table Omni uses: fact_dt_user_liveops_event_funnel). Joining this in means
-- participants with zero completions survive with NULL milestone fields - Omni's blank
-- "Furthest Milestone" bucket - instead of being silently excluded like a plain completions
-- query would. This is what makes the reproduction match Omni's join semantics exactly, not
-- just its context filter.
, funnel AS (
    SELECT DISTINCT
        user_id
    FROM trailmixgames-game-1.merger_prod_fact.fact_dt_user_liveops_event_funnel
    WHERE liveops_event_definition_id = target_liveops_event_definition_id
    AND liveops_iteration_id = target_liveops_iteration_id
)

, users_to_exclude AS (
    SELECT DISTINCT
        user_id
    FROM trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude
)

SELECT
      f.user_id
    , mp.dt
    , mp.days_since_event_start
    , mp.n_milestones_completed
    , mp.max_milestone_completed
    , mp.first_completion_ts
    , CURRENT_TIMESTAMP() AS loading_timestamp
FROM funnel f
LEFT JOIN milestone_progression_by_user_day mp ON f.user_id = mp.user_id
LEFT JOIN users_to_exclude u ON f.user_id = u.user_id
WHERE u.user_id IS NULL
