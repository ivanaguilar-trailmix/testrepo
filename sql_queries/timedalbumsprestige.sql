
DECLARE start_date DATE DEFAULT cast('2026-01-01' as date);

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
  #and cast(install_ts as date)>=current_date-lookback
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
    usd_ad_revenue_est / 0.85 AS gross_usd_ad_revenue
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

select ta.*, a.*, pl.*,
gross_usd_iap_revenue,
gross_usd_ad_revenue
from timed_album_users ta
join active_users a using (dt, user_id)
join player_level_all pl using (dt, user_id)
left join iap_rev using (dt,user_id)
left join ad_rev using(dt, user_id)
left join users_to_exclude ue using (user_id)
where 1=1
and ue.user_id is null