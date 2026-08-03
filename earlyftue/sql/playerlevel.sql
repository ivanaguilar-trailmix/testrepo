
DECLARE start_date1 DATE DEFAULT cast('{start_date1}' as date);
DECLARE end_date1 DATE DEFAULT cast('{end_date1}' as date);
DECLARE start_date2 DATE DEFAULT cast('{start_date2}' as date);
DECLARE end_date2 DATE DEFAULT cast('{end_date2}' as date);

DECLARE exclude_networks ARRAY<STRING> DEFAULT {exclude_networks};

with active_users as (
  select 
    dt,
    cast(install_ts as date) as install_dt,
    user_id,
    d.platform,
    ua.display_campaign_network,
    ua.acquisition_type,
    g.install_country_code as country_code,
    install_build_version,
  from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_build b using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device d using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session i using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_geo g using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dimchange_user_install_ua ua USING (user_id)
  where 1=1
  and dt>=start_date1
  and cast(install_ts as date)>=start_date1
  and dt<=end_date1
  and cast(install_ts as date)<=end_date1
  and d.platform in ('AND', 'IOS')
  and install_build_version < '0.76.0'
  and acquisition_type not in UNNEST(exclude_networks)
  and active = 1
  group by all
  union all 
  select 
    dt,
    cast(install_ts as date) as install_dt,
    user_id,
    d.platform,
    ua.display_campaign_network,
    ua.acquisition_type,
    g.install_country_code as country_code,
    install_build_version,
  from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_build b using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device d using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session i using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_geo g using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dimchange_user_install_ua ua USING (user_id)
  where 1=1
  and dt>=start_date2
  and cast(install_ts as date)>=start_date2
  and dt<=end_date2
  and cast(install_ts as date)<=end_date2
  and d.platform in ('AND', 'IOS')
  and install_build_version >= '0.76.0'
  and acquisition_type not in UNNEST(exclude_networks)
  and active = 1
  group by all

)

, player_level_all as (
  select distinct
    dt,
    max_level,
    user_id,
  from trailmixgames-game-1.merger_prod_fact.fact_dsi_user_progression_cumulative
  where 1=1
  and dt>=start_date1
)

, gameday_all as (
  select distinct
    dt,
    max_gameday,
    user_id,
  from trailmixgames-game-1.merger_prod_fact.fact_dsi_user_progression_cumulative
  where 1=1
  and dt>=start_date1
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
a.country_code,
date_trunc(a.install_dt, week) as install_dt_week,
date_trunc(a.install_dt, month) as install_dt_month,
date_diff(a.dt, a.install_dt, day) as days_since_install,
pl.max_level,
gd.max_gameday,
a.platform,
a.display_campaign_network,
a.acquisition_type,
a.install_build_version
from active_users a
join player_level_all pl using (dt, user_id)
join gameday_all gd using (dt, user_id)
left join users_to_exclude ue using (user_id)
where 1=1
and ue.user_id is null


