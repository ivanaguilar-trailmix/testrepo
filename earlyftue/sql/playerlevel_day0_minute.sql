
DECLARE start_date DATE DEFAULT cast('{start_date}' as date);
DECLARE install_cutoff_date DATE DEFAULT cast('{install_cutoff_date}' as date);
DECLARE end_date DATE DEFAULT cast('{end_date}' as date);
DECLARE minutes_cap INT64 DEFAULT {minutes_cap};

DECLARE exclude_networks ARRAY<STRING> DEFAULT {exclude_networks};

with active_users as (
  select
    user_id,
    d.platform,
    ua.acquisition_type,
    g.install_country_code as country_code,
    b.install_build_version,
    i.install_ts,
  from trailmixgames-game-1.merger_prod_dimensions.dim_user_install_build b
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device d using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session i using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_geo g using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dimchange_user_install_ua ua USING (user_id)
  where 1=1
  and cast(i.install_ts as date) >= start_date
  and cast(i.install_ts as date) <= install_cutoff_date
  and d.platform in ('AND', 'IOS')
  and ua.acquisition_type not in UNNEST(exclude_networks)
  group by all
)

, sessions as (
  select
    user_id,
    session_id,
    session_start_ts,
    session_end_ts,
  from trailmixgames-game-1.merger_prod_fact.fact_ssdt_user_ssid
  where 1=1
  and cast(session_start_ts as date) >= start_date
  and cast(session_start_ts as date) <= end_date
)

, session_level as (
  -- min_level/max_level are the level range spanned DURING a session, not a running
  -- total, so max_level is only known to be true as of session_end_ts, not session_start_ts.
  select distinct
    user_id,
    session_id,
    max_level,
  from trailmixgames-game-1.merger_prod_fact.fact_ssdt_user_ssid_level_progression
)

, users_to_exclude as (
  select distinct
  user_id
  from trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude
)

, sessions_with_level as (
  select
    a.user_id,
    s.session_id,
    s.session_end_ts,
    a.install_ts,
    sl.max_level,
    a.platform,
    a.acquisition_type,
    a.install_build_version
  from active_users a
  join sessions s using (user_id)
  join session_level sl using (user_id, session_id)
  left join users_to_exclude ue using (user_id)
  where 1=1
  and ue.user_id is null
)

select
user_id,
session_id,
session_end_ts,
install_ts,
timestamp_diff(session_end_ts, install_ts, MINUTE) as minutes_since_install,
-- running cumulative max per user, ordered by when each session actually ended,
-- since a single session's max_level is only a local peak, not the all-time high
max(max_level) over (
  partition by user_id
  order by session_end_ts
  rows between unbounded preceding and current row
) as max_level,
platform,
acquisition_type,
install_build_version
from sessions_with_level
where timestamp_diff(session_end_ts, install_ts, MINUTE) between 0 and minutes_cap
