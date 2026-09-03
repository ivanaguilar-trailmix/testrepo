-- Full reward-claim picture for GeneratorEvent-GardenGlory, correcting rewards.sql's scope.
-- rewards.sql deliberately filters to wallet_diff_array rows whose id matches
-- '%liveops-GeneratorEvent-GardenGlory%' - correct for isolating the event's own dedicated
-- currency (the point-milestone stage 0-15 ladder), but it silently excludes every other reward
-- track tied to this event, discovered by inspecting the raw reward_id space directly:
--   - path0/path1/path2: per-level rewards for each item-path (reward_id suffix
--     'pathN_itemM'), granting generic currencies (gems/energy) and cross-promo chests/packs -
--     never GardenGlory-branded, so rewards.sql's filter silently drops all of them.
--   - path0_complete/path1_complete/path2_complete: one-time bonus for fully finishing a
--     given item-path (reward_id suffix 'pathN_complete').
--   - complete: one-time bonus for finishing the full 16-milestone point ladder.
--   - the point-milestone ladder itself keeps granting real reward claims *past* milestone 15
--     (reward_id suffix a plain integer > 15) - the milestone system doesn't stop at 16, it
--     falls through to generic/cross-promo items once GardenGlory's own 16-slot reward table
--     is exhausted, rather than stopping. This directly updates LPD-261: overflow milestones
--     are not unrewarded, they're rewarded from an unrelated item pool.
--
-- dynamic_rewards_item_array is always empty for this event's claims (checked directly) - every
-- reward here flows through wallet_diff_array, so that's the only array unnested.
--
-- reward_instance_id is NOT a stable one-row-per-claim key for this table - it's shared across
-- many distinct reward_ids for the same user (a claim-batch/transaction id, not a per-reward
-- id), and a small number of users (16, found during investigation) have genuine repeat claims
-- of the identical reward_id (up to 3x). (user_id, reward_id) is the real claim grain; the
-- ROW_NUMBER() below keeps just the first instance of each so downstream counts aren't inflated
-- by that rare repeat-claim case.
WITH users_to_exclude AS (
    SELECT DISTINCT
        user_id
    FROM trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude
)

, claims AS (
    SELECT
          rc.event_ts
        , rc.user_id
        , rc.reward_id
        , rc.wallet_diff_array
        , REGEXP_EXTRACT(rc.reward_id, r'^\d+_(.*)$') AS reward_suffix
        , ROW_NUMBER() OVER (PARTITION BY rc.user_id, rc.reward_id ORDER BY rc.event_ts) AS rn
    FROM trailmixgames-game-1.merger_prod_raw_processing.rp_RewardClaimed rc
    LEFT JOIN users_to_exclude u ON rc.user_id = u.user_id
    WHERE rc.context = 'lop_GeneratorEvent'
    AND rc.reason = 'GeneratorEvent-GardenGlory'
    AND rc.reward_instance_id IS NOT NULL AND rc.reward_instance_id != ''
    AND DATE(rc.event_ts) >= '2026-08-17'
    AND u.user_id IS NULL
)

SELECT
      c.event_ts
    , DATE(c.event_ts) AS event_dt
    , c.user_id
    , c.reward_id
    , c.reward_suffix
    , CASE
        WHEN REGEXP_CONTAINS(c.reward_suffix, r'^[0-9]+$') AND CAST(c.reward_suffix AS INT64) <= 15 THEN 'ladder_0_15'
        WHEN REGEXP_CONTAINS(c.reward_suffix, r'^[0-9]+$') AND CAST(c.reward_suffix AS INT64) > 15 THEN 'ladder_16_plus'
        WHEN REGEXP_CONTAINS(c.reward_suffix, r'^path[0-9]+_item[0-9]+$') THEN REGEXP_EXTRACT(c.reward_suffix, r'^(path[0-9]+)_item[0-9]+$')
        WHEN REGEXP_CONTAINS(c.reward_suffix, r'^path[0-9]+_complete$') THEN c.reward_suffix
        WHEN c.reward_suffix = 'complete' THEN 'complete'
        ELSE CONCAT('other:', c.reward_suffix)
      END AS reward_track
    , (c.reward_suffix = 'complete' OR REGEXP_CONTAINS(c.reward_suffix, r'_complete$')) AS is_completion_bonus
    , w.id AS wallet_id
    , w.count AS wallet_count
    , CURRENT_TIMESTAMP() AS loading_timestamp
FROM claims c
LEFT JOIN UNNEST(c.wallet_diff_array) AS w
WHERE c.rn = 1
