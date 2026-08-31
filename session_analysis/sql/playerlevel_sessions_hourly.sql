DECLARE start_date DATE DEFAULT cast('{start_date}' as date);
DECLARE install_cutoff_date DATE DEFAULT cast('{install_cutoff_date}' as date);
DECLARE end_date DATE DEFAULT cast('{end_date}' as date);
DECLARE exclude_networks ARRAY<STRING> DEFAULT {exclude_networks};

with active_users as (
  select
    user_id,
    d.platform,
    ua.acquisition_type,
    g.install_country_code as country_code,
    i.install_ts,
  from trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session i
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device d using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_geo g using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dimchange_user_install_ua ua using (user_id)
  where 1=1
  and cast(i.install_ts as date) >= start_date
  and cast(i.install_ts as date) <= install_cutoff_date
  and d.platform in ('AND', 'IOS')
  and ua.acquisition_type not in unnest(exclude_networks)
  group by all
)

, sessions as (
  select
    user_id,
    session_id,
    session_start_ts,
  from trailmixgames-game-1.merger_prod_fact.fact_ssdt_user_ssid
  where 1=1
  and cast(session_start_ts as date) >= start_date
  and cast(session_start_ts as date) <= end_date
)

, users_to_exclude as (
  select distinct user_id
  from trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude
)

select
  a.user_id,
  s.session_id,
  s.session_start_ts,
  a.platform,
  a.acquisition_type,
  a.country_code
from active_users a
join sessions s using (user_id)
left join users_to_exclude ue using (user_id)
where 1=1
and ue.user_id is null
