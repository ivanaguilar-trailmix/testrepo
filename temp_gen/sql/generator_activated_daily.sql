DECLARE target_object_path STRING DEFAULT 'liveops-GeneratorEvent-GardenGlory-GenPath';

-- Per-user, per-day generator activation count - a daily-grain sibling of
-- sql/generator_activated.sql (which collapses to one row per user, first-activation-ever).
-- Needed to answer "how many times did a player use the generator on a given day," not just
-- "did they ever use it" - e.g. for an engagement-intensity cut (>= N activations that day).
WITH generator_activations AS (
    SELECT
          user_id
        , DATE(event_ts) AS dt
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
    , ga.dt
    , COUNT(*) AS n_activations
    , CURRENT_TIMESTAMP() AS loading_timestamp
FROM generator_activations ga
LEFT JOIN users_to_exclude u ON ga.user_id = u.user_id
WHERE u.user_id IS NULL
GROUP BY ALL
