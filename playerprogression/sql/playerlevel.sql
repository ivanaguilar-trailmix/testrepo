
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
  and cast(install_ts as date)>=start_date
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

, gameday_all as (
  select distinct
    dt,
    max_gameday,
    user_id,
  from trailmixgames-game-1.merger_prod_fact.fact_dsi_user_progression_cumulative
  where 1=1
  and dt>=start_date
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
a.install_dt,
date_trunc(a.install_dt, week) as install_dt_week,
date_trunc(a.install_dt, month) as install_dt_month,
date_diff(a.dt, a.install_dt, day) as days_since_install,
pl.max_level,
gd.max_gameday
from active_users a
join player_level_all pl using (dt, user_id)
join gameday_all gd using (dt, user_id)
left join users_to_exclude ue using (user_id)
where 1=1
and ue.user_id is null


