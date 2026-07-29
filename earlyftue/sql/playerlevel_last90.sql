
DECLARE start_date DATE DEFAULT cast('{start_date}' as date);
DECLARE install_cutoff_date DATE DEFAULT cast('{install_cutoff_date}' as date);
DECLARE end_date DATE DEFAULT cast('{end_date}' as date);

DECLARE exclude_networks ARRAY<STRING> DEFAULT {exclude_networks};

with active_users as (
  select
    dt,
    cast(install_ts as date) as install_dt,
    user_id,
    d.platform,
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
  and dt>=start_date
  and cast(install_ts as date)>=start_date
  and dt<=end_date
  and cast(install_ts as date)<=install_cutoff_date
  and d.platform in ('AND', 'IOS')
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
a.country_code,
date_diff(a.dt, a.install_dt, day) as days_since_install,
pl.max_level,
a.platform,
a.acquisition_type,
a.install_build_version
from active_users a
join player_level_all pl using (dt, user_id)
left join users_to_exclude ue using (user_id)
where 1=1
and ue.user_id is null
