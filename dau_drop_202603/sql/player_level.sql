DECLARE lookback INT64 DEFAULT {lookback};

with active_users as (
  select 
    dt,
    cast(install_ts as date) as install_dt,
    user_id,
    platform,
    if (cast(install_ts as date)>=current_date-lookback, 'New', 'Existing') as user_type
  from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session using (user_id)
  where 1=1
  and dt>=current_date-lookback
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
  and dt>=current_date-lookback
  and max_level<=150
)

select 
  dt,
  max_level,
  platform,
  user_id,
  #player_level_segment,
  count(distinct a.user_id) as active_users,
from active_users a 
inner join player_level_all pl using (dt, user_id)
#inner join user_segments us using (dt, user_id)
where 1=1
group by all
order by 1,2,3,4