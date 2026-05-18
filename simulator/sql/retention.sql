-- Retention curve by platform and day-since-install (dx).


DECLARE start_date DATE DEFAULT CAST('{start_date}' AS DATE);

WITH activity AS (
  SELECT dt, user_id, platform, CAST(install_ts AS DATE) AS install_dt,
  FROM trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
  JOIN trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device USING (user_id)
  JOIN trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session USING (user_id)
  WHERE dt >= start_date
  and dt < CURRENT_DATE()
  and platform in ('AND', 'IOS')
  and active = 1
),

installs AS (
  SELECT DISTINCT
    user_id,
    install_dt,
    platform
  FROM activity
  WHERE install_dt = dt
    AND install_dt >= start_date
),

cohort_sizes AS (
  SELECT install_dt, platform, COUNT(DISTINCT user_id) AS cohort_size
  FROM installs
  GROUP BY 1, 2
),

cohort_activity AS (
  SELECT
    i.install_dt,
    i.platform,
    DATE_DIFF(a.dt, i.install_dt, DAY) AS dx,
    COUNT(DISTINCT a.user_id) AS retained
  FROM installs i
  JOIN activity a ON a.user_id = i.user_id
   AND DATE_DIFF(a.dt, i.install_dt, DAY) IN (0, 1, 3, 7, 14, 21, 30, 60, 90, 180, 365, 1000, 1800)
   AND DATE_DIFF(current_date-1, i.install_dt, DAY) >= DATE_DIFF(a.dt, i.install_dt, DAY)
  GROUP BY 1, 2, 3
)

SELECT
  ca.install_dt,
  ca.dx,
  ca.platform,
  SUM(cs.cohort_size) as cohort_size,
  SUM(ca.retained) as retained_size,
  SAFE_DIVIDE(SUM(ca.retained), SUM(cs.cohort_size)) AS retention_rate
FROM cohort_activity ca
JOIN cohort_sizes cs USING (install_dt, platform)
GROUP BY ALL
ORDER BY 1,2
