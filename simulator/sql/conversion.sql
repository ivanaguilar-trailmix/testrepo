-- Cumulative conversion-to-payer rate by platform and day-since-install (dx).


DECLARE start_date DATE DEFAULT cast('{start_date}' as date);

WITH installs AS (
  SELECT DISTINCT 
    user_id,
    cast(install_ts as date) as install_dt,
    platform,
    first_purchase_dt,
  from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
  left join trailmixgames-game-1.merger_prod_dimensions.dim_user_first_purchase using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session using (user_id)
  WHERE 1=1
    AND cast(install_ts as date)>=start_date
    AND dt>=start_date
    AND cast(install_ts as date) = dt
    and dt < CURRENT_DATE()
    and platform in ('AND', 'IOS')
    and active = 1
  )


, cohort_conversions AS (
    SELECT
        i.install_dt,
        i.platform,
        dx_check_dx,
        COUNT(DISTINCT i.user_id) AS cohort_size,
        COUNT(DISTINCT IF(DATE_DIFF(i.first_purchase_dt, i.install_dt, DAY) <= dx_check_dx, i.user_id, NULL)) AS converters
    FROM installs i
    CROSS JOIN UNNEST([0, 1, 3, 7, 14, 30, 60, 90, 180, 365, 1000, 1800]) AS dx_check_dx
    WHERE 1=1 
    AND DATE_DIFF(CURRENT_DATE() - 1, i.install_dt, DAY) >= dx_check_dx
    GROUP BY all
)

SELECT
    install_dt,
    dx_check_dx,
    platform,
    SAFE_DIVIDE(SUM(converters), SUM(cohort_size)) AS conversion_rate
FROM cohort_conversions
GROUP BY all
ORDER BY install_dt, dx_check_dx, platform

