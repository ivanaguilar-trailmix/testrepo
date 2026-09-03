DECLARE window_after_event_ends INT64 DEFAULT 14;
DECLARE target_liveops_event_definition_id STRING DEFAULT 'GeneratorEvent-GardenGlory';
DECLARE target_liveops_iteration_id STRING DEFAULT '202608171000';

-- Exact points balance at (or immediately before) each milestone completion, via an as-of join
-- (carry-forward last known balance) between the milestone-completion event stream and the
-- exact-timestamp points-event stream - not a daily approximation. Points are tracked as
-- 'generatorEventPoints' in rp_ts_user_events_liveops_economy (the event-level source
-- fact_dt_user_liveops_economy itself is built from - confirmed by reading the dbt model
-- directly), a persistent, never-spent running balance (outflow is always 0 for this event).
--
-- No milestone upper bound - covers the full range (1 through whatever the data has, currently
-- up to 162) in one query, so both the reward-eligible ladder and the >16 overflow zone can be
-- compared without a second pull.
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

, milestone_completions AS (
    SELECT
          user_id
        , milestone
        , MIN(event_ts) AS event_ts
    FROM latest_milestone_progression
    WHERE event_ts BETWEEN CAST(liveops_event_start_ts AS DATETIME) AND DATE_ADD(CAST(liveops_event_end_ts AS DATETIME), INTERVAL window_after_event_ends day)
    GROUP BY ALL
)

, points_events AS (
    SELECT
          user_id
        , event_ts
        , balance
    FROM trailmixgames-game-1.merger_prod_raw_processing.rp_ts_user_events_liveops_economy
    WHERE resource_id = 'generatorEventPoints'
    AND DATE(event_ts) BETWEEN '2026-08-17' AND '2026-09-14'
)

, combined AS (
    SELECT user_id, event_ts, balance, CAST(NULL AS INT64) AS milestone
    FROM points_events
    UNION ALL
    SELECT user_id, event_ts, CAST(NULL AS INT64) AS balance, milestone
    FROM milestone_completions
)

, carried AS (
    SELECT
          user_id
        , event_ts
        , milestone
        -- Carries the last known balance forward onto each milestone-completion row in the
        -- merged, time-sorted stream - the standard as-of join pattern (not a groupby/cummax,
        -- which only works within a single stream, not across two merged ones).
        , LAST_VALUE(balance IGNORE NULLS) OVER (
            PARTITION BY user_id ORDER BY event_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS points_balance_at_or_before
    FROM combined
)

SELECT
      milestone
    , user_id
    , points_balance_at_or_before
FROM carried
WHERE milestone IS NOT NULL
