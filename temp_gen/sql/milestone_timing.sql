DECLARE window_after_event_ends INT64 DEFAULT 14;
DECLARE target_liveops_event_definition_id STRING DEFAULT 'GeneratorEvent-GardenGlory';
DECLARE target_liveops_iteration_id STRING DEFAULT '202608171000';

-- Per-user, per-milestone first-completion timestamp for the point-milestone ladder (same
-- context filter as milestones.sql). Kept at this grain rather than collapsed to user-day like
-- milestones.sql's final SELECT - the per-milestone timestamp is exactly what's needed to compute
-- time-between-milestones downstream, which the day-collapsed version throws away.
--
-- No upper bound on milestone number - pulls the full range (including the >16 tail from
-- LPD-261) so the same pickle can support both the reward-eligible (1-16) timing question and a
-- later pacing comparison against the >16 overflow, without a second query.
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
        , liveops_event_start_ts
        , milestone
        , MIN(event_ts) AS min_ts
    FROM latest_milestone_progression
    -- same window as milestones.sql - completion between event start and 14 days past event end
    WHERE event_ts BETWEEN CAST(liveops_event_start_ts AS DATETIME) AND DATE_ADD(CAST(liveops_event_end_ts AS DATETIME), INTERVAL window_after_event_ends day)
    GROUP BY ALL
)

, users_to_exclude AS (
    SELECT DISTINCT
        user_id
    FROM trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude
)

SELECT
      mp.user_id
    , mp.liveops_event_start_ts
    , mp.milestone
    , mp.min_ts AS completion_ts
    , CURRENT_TIMESTAMP() AS loading_timestamp
FROM int_ts_user_liveops_milestone_progression mp
LEFT JOIN users_to_exclude u ON mp.user_id = u.user_id
WHERE u.user_id IS NULL
