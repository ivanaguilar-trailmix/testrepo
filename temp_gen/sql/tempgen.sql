DECLARE window_after_event_ends INT64 DEFAULT 14;

-- get latest data
WITH latest_item_path_progression AS (
        SELECT
              user_id
            , liveops_event_type_id
            , liveops_event_definition_id
            , liveops_iteration_id
            , liveops_event_start_ts
            , liveops_event_end_ts
            , liveops_event_unique_id
            /*
                `tier` is the raw 0-based path index and `type` is the client's label for it
                (generatorPath / primaryPath / secondaryPath, added in 0.81 by MERGE-16154).

                MERGE-16154 specced only ctx and type, so `type` is the field the client is
                contracted to get right and `tier` is incidental - it is not guaranteed stable
                across iterations. `tier` is therefore left NULLABLE rather than coalesced to 0:
                0 is a real path index (the generator), so defaulting to it would both make a
                missing tier indistinguishable from a genuine generator-path discovery in a
                column consumers group by, and collide with one on the grain. The uniqueness
                tests on this model and on the fact are keyed on path_type, the contractual
                field, so a client-side mislabel still surfaces as a duplicate.
            */
            , tier AS path_tier
            , type AS path_type
            /*
                `milestone` arrives from rp_LOpsMilestoneComplete already converted to the 1-based
                level players see (rp adds +1 for every non-SeasonPass milestone event, which
                includes this one). It lines up exactly with rp_ObjectUnlocked.object_level -
                do not offset it again.
            */
            , milestone AS item_level
            -- not doing min at this stage (b/c of incremental logic) but will do it in next CTE after union
            , event_ts
        FROM trailmixgames-game-1.merger_prod_raw_processing.rp_LOpsMilestoneComplete
        -- the item-path ladder has its own context; point-milestone rows use goal / goalCompleted
        WHERE context = 'itemPath'
        /*
            Filter out events that we won't know the start/end ts for. Split in two on purpose:
            NULL NOT LIKE 'Unknown%' evaluates to NULL and WHERE treats that as excluded, so a
            single clause drops a failed dim join and a deliberately tagged unknown event
            through the same condition and leaves them indistinguishable afterwards. Stating
            the NULL case separately gives a future test something to assert on - it is the
            mechanism behind an unexpectedly empty table.
        */
        AND liveops_event_type_ID IS NOT NULL
        AND liveops_event_type_ID NOT LIKE 'Unknown%'
    -- if this table already exists, get new events within partition, and union distinct previous events in partition + max event duration (to be able to correctly do min_ts)
        AND DATE(event_ts) >= '2026-08-17'
)

, int_ts_user_liveops_item_path_progression AS (
    SELECT
          user_id
        , liveops_event_type_id
        , liveops_event_definition_id
        , liveops_iteration_id
        , liveops_event_start_ts
        , liveops_event_end_ts
        , liveops_event_unique_id
        , path_tier
        , path_type
        , item_level
        , MIN(event_ts) AS min_ts
        , CURRENT_TIMESTAMP() AS loading_timestamp
    FROM latest_item_path_progression
    -- keep only rows where the discovery happened between the event starting and within 2 weeks of it ending
    WHERE event_ts BETWEEN CAST(liveops_event_start_ts AS DATETIME) AND DATE_ADD(CAST(liveops_event_end_ts AS DATETIME), INTERVAL window_after_event_ends day)
    GROUP BY ALL
)


, users_to_exclude as (
select distinct
user_id
from trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude
where 1=1
)

SELECT
      pp.user_id
    , DATE(min_ts) AS dt
    , liveops_event_type_id
    , liveops_event_definition_id
    , liveops_iteration_id
    , liveops_event_start_ts
    , liveops_event_end_ts
    , path_tier
    , path_type
    -- TIMESTAMP_DIFF on the raw timestamps, matching every sibling liveops fact
    -- (fact_dt_user_liveops_event_engaged, _ms_/_rnk_event_progression, _event_funnel), so this
    -- shared column is computed the same way everywhere.
    , TIMESTAMP_DIFF(min_ts, CAST(liveops_event_start_ts AS DATETIME), DAY) AS days_since_event_start
    , COUNT(DISTINCT item_level) AS n_levels_discovered
    , MAX(item_level) AS max_item_level_reached
    -- kept so time-to-level / early-finisher analysis does not have to go back to the int_ts model
    , MIN(min_ts) AS first_discovery_ts
    , CURRENT_TIMESTAMP() AS loading_timestamp
FROM int_ts_user_liveops_item_path_progression pp
LEFT JOIN users_to_exclude u ON pp.user_id = u.user_id
WHERE 1=1
and date(min_ts)>='2026-08-17'
and u.user_id IS NULL
GROUP BY ALL