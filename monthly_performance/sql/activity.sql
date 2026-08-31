
DECLARE start_date DATE DEFAULT cast('{start_date}' as date);
DECLARE end_date DATE DEFAULT cast('{end_date}' as date);


DECLARE exclude_networks ARRAY<STRING> DEFAULT {exclude_networks};


with pseg_avg as (
select 
  cast(effective_ts as date) as effective_dt, 
  cast(expiry_ts as date) as expiry_dt,
  user_id, 
  segment_type,
  segment_value
from trailmixgames-game-1.merger_prod_dimensions.dimchange_user_dailyoffers_psegs
where 1=1
-- overlap check, not "changed during the window" — a membership that started before
-- start_date and never changed again is still the user's active segment throughout the
-- window, so it must not be dropped here (matches how loyalty_segment/payer_value_segment/
-- etc. are joined further down: as-of validity, not a change-event filter).
and effective_ts <= end_date and (expiry_ts is null or expiry_ts >= start_date)
and segment_type = 'DailyOffers-30d_Avg'
--and segment_type = 'DailyOffers-30d_LastPurchase'
--and segment_type = 'DailyOffers-30d_Count'
)

, pseg_tcount as (
select
  cast(effective_ts as date) as effective_dt,
  cast(expiry_ts as date) as expiry_dt,
  user_id,
  segment_type,
  segment_value
from trailmixgames-game-1.merger_prod_dimensions.dimchange_user_dailyoffers_psegs
where 1=1
and effective_ts <= end_date and (expiry_ts is null or expiry_ts >= start_date)
and segment_type = 'DailyOffers-30d_Count'
--and segment_type = 'DailyOffers-30d_LastPurchase'
--and segment_type = 'DailyOffers-30d_Count'
)

, pseg_lastpurchase as (
select
  cast(effective_ts as date) as effective_dt,
  cast(expiry_ts as date) as expiry_dt,
  user_id,
  segment_type,
  segment_value
from trailmixgames-game-1.merger_prod_dimensions.dimchange_user_dailyoffers_psegs
where 1=1
and effective_ts <= end_date and (expiry_ts is null or expiry_ts >= start_date)
--and segment_type = 'DailyOffers-30d_Avg'
and segment_type = 'DailyOffers-30d_LastPurchase'
--and segment_type = 'DailyOffers-30d_Count'
)

, active_users as (
  select 
    dt,
    cast(install_ts as date) as install_dt,
    a.user_id,
    --d.platform,
    --ua.display_campaign_network,
    --ua.acquisition_type,
    --g.install_country_code as country_code,
    --install_build_version,
  from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity a
  --join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_build b using (user_id)
  --join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device d using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session i using (user_id)
  --join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_geo g using (user_id)
  --join trailmixgames-game-1.merger_prod_dimensions.dimchange_user_install_ua ua USING (user_id)
  where 1=1
  and dt between start_date and end_date
  --and cast(install_ts as date) between start_date and end_date
  --and d.platform in ('AND', 'IOS')
  --and acquisition_type not in UNNEST(exclude_networks)
  and a.active = 1
  group by all

)

-- AD REV
, ad_rev AS (
  SELECT distinct 
    user_id, 
    dt, 
    usd_ad_revenue_est / 0.85 AS usd_gross_ad_revenue,
    usd_ad_revenue_est,
  FROM trailmixgames-game-1.merger_prod_fact.fact_dt_user_ad_revenue
  WHERE 1=1
  and dt>=start_date
  and dt<=end_date
)

-- IAP REV
, iap_rev AS (
  SELECT  distinct 
    user_id, 
    dt, 
    usd_iap_revenue,
    usd_net_iap_revenue,
    1 AS payer_flag
  FROM trailmixgames-game-1.merger_prod_fact.fact_dt_user_iap_revenue
  WHERE 1=1
  and dt>=start_date
  and dt<=end_date
)


, users_to_exclude as (
select distinct
user_id
from trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude
where 1=1
)

select 
  a.user_id,
  a.dt,
  date_trunc(a.dt, week) as dt_week,
  date_trunc(a.dt, month) as dt_month,
  a.install_dt,
  --a.country_code,
  date_trunc(a.install_dt, week) as install_dt_week,
  date_trunc(a.install_dt, month) as install_dt_month,
  date_diff(a.dt, a.install_dt, day) as days_since_install,
  --pl.max_level,
  --gd.max_gameday,
  --a.platform,
  --a.display_campaign_network,
  --a.acquisition_type,
  --a.install_build_version,
  iap.usd_net_iap_revenue AS usd_net_iap_revenue,
  adr.usd_ad_revenue_est AS usd_net_ad_revenue,
  COALESCE(iap.payer_flag, 0) AS payer_flag,

from active_users a
left join ad_rev adr using (user_id, dt)
left join iap_rev iap using (user_id, dt)
--join player_level_all pl using (dt, user_id)
--join gameday_all gd using (dt, user_id)
left join users_to_exclude ue using (user_id)
where 1=1
and ue.user_id is null


