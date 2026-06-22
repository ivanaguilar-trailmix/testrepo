-- Retention curve by platform and day-since-install (dx).


DECLARE start_date1 DATE DEFAULT cast('{start_date1}' as date);
DECLARE end_date1 DATE DEFAULT cast('{end_date1}' as date);
DECLARE start_date2 DATE DEFAULT cast('{start_date2}' as date);
DECLARE end_date2 DATE DEFAULT cast('{end_date2}' as date);

WITH activity AS (
  select 
    dt,
    cast(install_ts as date) as install_dt,
    user_id,
    d.platform,
    ua.acquisition_type,
    install_build_version
  from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_build b using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device d using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session i using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dimchange_user_install_ua ua USING (user_id)
  where 1=1
  and dt>=start_date1
  and cast(install_ts as date)>=start_date1
  and dt<=end_date1
  and cast(install_ts as date)<=end_date1
  and d.platform in ('AND', 'IOS')
  and install_build_version in ('0.74.0','0.75.0')
  and display_campaign_network != 'CPE'
  and active = 1
  group by all
  union all 
  select 
    dt,
    cast(install_ts as date) as install_dt,
    user_id,
    d.platform,
    ua.acquisition_type,
    install_build_version
  from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_build b using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device d using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session i using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dimchange_user_install_ua ua USING (user_id)
  where 1=1
  and dt>=start_date2
  and cast(install_ts as date)>=start_date2
  and dt<=end_date2
  and cast(install_ts as date)<=end_date2
  and d.platform in ('AND', 'IOS')
  and install_build_version in ('0.76.0', '0.77.0', '0.78.0', '0.79.0', '0.80.0')
  and display_campaign_network != 'CPE'
  and active = 1
  group by all
),

installs AS (
  SELECT DISTINCT
    user_id,
    install_dt,
    a.platform,
    a.install_build_version
  FROM activity a
  WHERE 1=1 
    AND install_dt = dt
    AND install_dt >= start_date1
    AND install_dt <= end_date2
), 

cohort_sizes AS (
  SELECT 
    --install_dt, 
    platform,
    install_build_version,
    COUNT(DISTINCT user_id) AS cohort_size
  FROM installs
  GROUP BY ALL
),

cohort_activity AS (
  SELECT
    --i.install_dt,
    i.platform,
    i.install_build_version,
    DATE_DIFF(a.dt, i.install_dt, DAY) AS dx,
    COUNT(DISTINCT a.user_id) AS retained
  FROM installs i
  JOIN activity a ON a.user_id = i.user_id
   AND DATE_DIFF(a.dt, i.install_dt, DAY) IN (0, 1, 3, 7, 14, 21, 30, 60, 90, 180, 365, 1000, 1800)
   AND DATE_DIFF(end_date2, i.install_dt, DAY) >= DATE_DIFF(a.dt, i.install_dt, DAY)
  GROUP BY ALL
)

SELECT
  --ca.install_dt,
  ca.dx,
  ca.platform,
  --install_build_version,
  if(ca.install_build_version >= '0.76.0', 'B.Post-FTUE revamp', 'A.Pre-FTUE revamp') as FTUE_flag,
  SUM(cs.cohort_size) as cohort_size,
  SUM(ca.retained) as retained_size,
  SAFE_DIVIDE(SUM(ca.retained), SUM(cs.cohort_size)) AS retention_rate
FROM cohort_activity ca
JOIN cohort_sizes cs USING (install_build_version, platform)
GROUP BY ALL
ORDER BY 1,2
