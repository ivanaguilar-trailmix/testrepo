DECLARE target_object_path STRING DEFAULT 'liveops-GeneratorEvent-GardenGlory-GenPath';

-- The additional ("temp") generator itself firing an activation/collection event - this is the
-- only raw signal found for "did this player actually have the generator on," since there is no
-- separate enabled/exists flag anywhere in the warehouse. A row here means the generator existed
-- on the player's board AND was collected from at least once; it is close to but not identical to
-- tempgen.sql's item-path discovery population (activating doesn't strictly require reaching a
-- level-1 item-path milestone, and vice versa) - see [[project_tempgen]].
WITH generator_activations AS (
    SELECT
          user_id
        , DATE(event_ts) AS dt
        , event_ts
    FROM trailmixgames-game-1.merger_prod_raw_processing.rp_unnested_generator_activated_array
    WHERE generator_activated_object_path = target_object_path
    AND DATE(event_ts) BETWEEN '2026-08-17' AND '2026-08-31'
)

, users_to_exclude AS (
    SELECT DISTINCT
        user_id
    FROM trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude
)

SELECT
      ga.user_id
    , MIN(ga.dt) AS first_activation_dt
    , MIN(ga.event_ts) AS first_activation_ts
    , COUNT(*) AS n_activations
    , CURRENT_TIMESTAMP() AS loading_timestamp
FROM generator_activations ga
LEFT JOIN users_to_exclude u ON ga.user_id = u.user_id
WHERE u.user_id IS NULL
GROUP BY ALL
