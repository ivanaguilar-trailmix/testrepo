
DECLARE start_date DATE DEFAULT cast('{start_date}' as date);

with active_users as (
  select 
    dt,
    cast(install_ts as date) as install_dt,
    user_id,
    platform,
  from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session using (user_id)
  where 1=1
  and dt>=start_date
  group by all

)

, arraydates as (
      select 
        user_id,
        dt,
        platform,
        array_agg(dt) over (partition by user_id, platform) as nextval
    from active_users
)

, getdayxflags as (
    select
      user_id,
      platform,
      dt,
      date_add(dt, interval 1 day) in unnest(nextval) day_1,
      date_add(dt, interval 3 day) in unnest(nextval) day_3,
      date_add(dt, interval 7 day) in unnest(nextval) day_7,
      date_add(dt, interval 14 day) in unnest(nextval) day_14,
      date_add(dt, interval 28 day) in unnest(nextval) day_28
    from arraydates
)

, flagreturnrate as (
  select 
    dt,
    user_id,
    if(day_1, user_id, null) as user_id_d1,
    if(day_3, user_id, null) as user_id_d3,
    if(day_7, user_id, null) as user_id_d7,
    if(day_14, user_id, null) as user_id_d14,
    if(day_28, user_id, null) as user_id_d28
  from getdayxflags
  group by all
)

, player_level_all as (
  select distinct
    dt,
    max_level,
    user_id,
  from trailmixgames-game-1.merger_prod_fact.fact_dsi_user_progression_cumulative
  where 1=1
  and dt>=start_date
)


-- AD REV
, ad_rev AS (
  SELECT 
    user_id, 
    dt, 
    usd_ad_revenue_est / 0.85 AS gross_usd_ad_revenue,
    usd_ad_revenue_est AS net_usd_ad_revenue
  FROM trailmixgames-game-1.merger_prod_fact.fact_dt_user_ad_revenue
  WHERE 1=1
  and dt>=start_date
)

-- IAP REV
, iap_rev AS (
  SELECT 
    user_id, 
    dt, 
    usd_iap_revenue AS gross_usd_iap_revenue, 
    usd_net_iap_revenue AS net_usd_iap_revenue,
    1 AS payer_flag
  FROM trailmixgames-game-1.merger_prod_fact.fact_dt_user_iap_revenue
  WHERE 1=1
  and dt>=start_date
)

, timed_album_users as (
select 
*
from trailmixgames-game-1.merger_prod_fact.fact_dse_user_timed_album_progression_cumulative
where 1=1
and dt>=start_date
order by user_id, dt
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
pl.max_level,
ta.* except(user_id, dt),
gross_usd_iap_revenue,
net_usd_iap_revenue,
gross_usd_ad_revenue,
net_usd_ad_revenue,
-- return rates
user_id_d1,
user_id_d3,
user_id_d7,
user_id_d14,
user_id_d28 
from active_users a
join player_level_all pl using (dt, user_id)
left join flagreturnrate using (dt, user_id)
left join timed_album_users ta using (dt, user_id)
left join iap_rev using (dt,user_id)
left join ad_rev using(dt, user_id)
left join users_to_exclude ue using (user_id)
where 1=1
and ue.user_id is null