DECLARE start_date DATE DEFAULT cast('{start_date}' as DATE);
DECLARE end_date DATE DEFAULT cast('{end_date}' as DATE);
DECLARE dayx INT64 DEFAULT 28;

WITH date_array AS(
  SELECT
    *
  FROM UNNEST(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS dt
)

, user_lifetime as (
select distinct
  dt,
  cast(install_ts as date) as install_dt,
  user_id
from trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session
cross join date_array
where 1=1
  and cast(install_ts as date) between start_date and end_date
  and dt>=cast(install_ts as date)
)

, active_users as (
  select distinct
    dt,
    user_id
  from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
  where 1=1
  and dt between start_date and end_date
)


, ltv_base as(
  SELECT
    ul.user_id,
    ul.install_dt,
    ul.dt,
    DATE_DIFF(ul.dt, ul.install_dt, DAY) AS days_since_install,
    coalesce(rev.usd_iap_revenue_cumu, 0) AS usd_iap_revenue_cumu,
    coalesce(rev.usd_iap_revenue_exc_seasons_cumu, 0) AS usd_iap_revenue_exc_seasons_cumu,
    coalesce(rev.usd_net_iap_revenue_cumu, 0) AS usd_net_iap_revenue_cumu,
    coalesce(rev.usd_net_iap_revenue_exc_seasons_cumu, 0) AS usd_net_iap_revenue_exc_seasons_cumu
  FROM user_lifetime ul
  LEFT JOIN trailmixgames-game-1.merger_prod_fact.fact_dsi_user_iap_revenue_cumulative rev using(user_id, dt)
  --LEFT JOIN active_users au using(user_id, dt)
  WHERE 1=1
    and ul.install_dt between start_date and end_date
    --and ul.dt between start_date and end_date
    --and days_since_install <= dayx
    and date_diff(current_date, ul.install_dt, day) > dayx
)

select * 
from ltv_base
where 1=1
  and days_since_install <= dayx