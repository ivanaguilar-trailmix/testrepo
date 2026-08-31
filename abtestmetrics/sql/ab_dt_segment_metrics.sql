DECLARE experiment_name_var STRING DEFAULT '{experiment_name}';

-- Static/production snapshot: the pre-aggregated table ab_trends.ipynb itself reads from
-- (see data_pipeline_dbt/projects/merger/models/refactored/ab_test/ab_dt_segment_metrics.sql).
-- Rebuilt daily - a given historical dt's row reflects whatever data had landed as of that
-- table's last refresh, unlike the notebook's own live pull against raw tables above, which
-- always reflects late-arriving events up to the moment it's run. Comparing the two shows
-- how much a day's numbers have moved since the last scheduled refresh.
SELECT
      dt
    , variant
    , metric
    , total_users_assigned
    , dau_assigned
    , dau
    , sum_metric
    , avg_active_metric
    , sd_active_metric
    , loading_timestamp
FROM trailmixgames-game-1.merger_prod_ab.ab_dt_segment_metrics
WHERE experiment_name = experiment_name_var
AND payer_preassign = 'all'
AND dsi_segment_assign = 'all'
AND loyalty_segment_assign = 'all'
