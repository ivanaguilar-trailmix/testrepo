DECLARE baseline_date DATE DEFAULT cast('{abteststartdate}' AS DATE);
DECLARE lookback INT64 DEFAULT {lookback}; 
DECLARE experiment_name_var STRING DEFAULT '{experiment_name}';
    
WITH open_tests AS (
    SELECT experiment_name
         , min_start_dt
         , COALESCE(end_dt, CURRENT_DATE()-1) AS end_dt
         , IF(experiment_name IN ('OrderSystemv3'), 1, 0) AS required_matching
    FROM `trailmixgames-game-1`.`merger_prod_dimensions`.`dim_experiments`
    GROUP BY ALL
    -- for incremental runs, filter on ab tets that are still open
    
    HAVING end_dt >= DATE_SUB(DATE(baseline_date), INTERVAL 7 DAY)
    -- for full refresh filter on ab tests since the min ab test date that we want to analyse
    
)

-- grab matched samples from relevant ab tables that have the matched sampled
, matched_sample_data AS (
    SELECT user_id
          , experiment_name
          , variant
          , exposure_dt
          , matched_sample
      FROM `trailmixgames-game-1`.`merger_prod_ab`.`ab_expdt_matched_users` AS ex
      
      WHERE exposure_dt >= DATE_SUB(DATE(baseline_date), INTERVAL 7 + 90 DAY)
      
)

, max_exposure AS (
  SELECT experiment_name
        , MAX(exposure_dt) AS max_matched_exposure_dt
  FROM matched_sample_data
  GROUP BY ALL
)

, get_abtest_variants as (
    SELECT
        ex.user_id
      , op.experiment_name
      , ex.user_exposure_dt AS exposure_dt
      , ex.assigned_dt
      , op.min_start_dt
      , op.end_dt
--      , op.required_matching
--      , max_match.max_matched_exposure_dt
--      , COALESCE(ms.matched_sample, 0) AS matched_sample
--      , ex.dt_reached_level_gate
      , ex.experiment_variant AS variant
--      , ex.dsi_segment_assign
--    , ex.payer_preassign
--    , ex.loyalty_segment_assign
    FROM `trailmixgames-game-1`.`merger_prod_fact`.`fact_dt_user_experiment_segments` ex
    INNER JOIN open_tests op USING(experiment_name)
    LEFT JOIN matched_sample_data ms USING(experiment_name, user_id)
    LEFT JOIN max_exposure max_match USING(experiment_name)
    WHERE ex.bool_exclude_multi_variants IS FALSE
      -- for ab tests that require matching:
      AND (CASE WHEN op.required_matching = 1 THEN
                    -- filter on users in the matched sample up to max_matched_exposure_dt (matching done on sample available at that time)
                    CASE WHEN ex.user_exposure_dt <= COALESCE(max_match.max_matched_exposure_dt, DATE('2025-02-26')) THEN COALESCE(ms.matched_sample, 0)
                          -- after that date, keep any newly exposed users (since it'll mainly be fresh installs, they won't have pre-test bias so don't need matching)
                         ELSE 1
                     END
                -- if the ab test does not require matching, keep everyone
              ELSE 1
         END) = 1    
      -- filter on longest ab test period to ensure we're capturing all users assigned to taht test
      AND ex.assigned_dt >= DATE_SUB(DATE(baseline_date), INTERVAL lookback DAY)
      AND experiment_name = experiment_name_var
)

select * from get_abtest_variants