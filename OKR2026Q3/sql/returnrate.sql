
DECLARE startdate date default cast('{start_date}' as date);
DECLARE enddate date default cast('{end_date}' as date);
-- Loyalty segments to include; empty array = no filter (all segments, including new installs).
DECLARE loyalty_segments ARRAY<STRING> DEFAULT {loyalty_segments};

with activity as (
select
  a.user_id,
  a.dt,
  d.platform,
from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity a
join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device d using (user_id)
left join trailmixgames-game-1.merger_prod_dimensions.dimchange_user_loyalty_segment us on (
    a.user_id = us.user_id
    and a.dt >= us.effective_dt
    and a.dt < coalesce(us.expiry_dt, current_date()))
where 1=1
and a.dt between startdate and enddate
and d.platform in ('AND','IOS')
and (array_length(loyalty_segments) = 0 or us.loyalty_segment in unnest(loyalty_segments))
)

, arraydates as (
      select 
        user_id,
        dt,
        platform,
        array_agg(dt) over (partition by user_id, platform) as nextval
    from activity
)

, getdayxflags as (
    select
      user_id,
      platform,
      dt,
      -- exact-day return: active on that specific day
      date_add(dt, interval 1 day) in unnest(nextval) day_1,
      date_add(dt, interval 3 day) in unnest(nextval) day_3,
      date_add(dt, interval 7 day) in unnest(nextval) day_7,
      date_add(dt, interval 14 day) in unnest(nextval) day_14,
      date_add(dt, interval 28 day) in unnest(nextval) day_28,
      -- within-window return: active on any day up to and including that day (churn-complement view)
      exists(select 1 from unnest(nextval) d where d > dt and d <= date_add(dt, interval 1 day)) within_1,
      exists(select 1 from unnest(nextval) d where d > dt and d <= date_add(dt, interval 3 day)) within_3,
      exists(select 1 from unnest(nextval) d where d > dt and d <= date_add(dt, interval 7 day)) within_7,
      exists(select 1 from unnest(nextval) d where d > dt and d <= date_add(dt, interval 14 day)) within_14,
      exists(select 1 from unnest(nextval) d where d > dt and d <= date_add(dt, interval 28 day)) within_28
    from arraydates
)

, calculatereturnrate as (
  select 
    dt,
    --platform,
    count(distinct user_id) dau,
    count(distinct if(day_1, user_id, null))/count(distinct user_id) retention_day_01,
    count(distinct if(day_1, user_id, null)) as dau_d1,
    count(distinct if(day_3, user_id, null))/count(distinct user_id) retention_day_03,
    count(distinct if(day_3, user_id, null)) as dau_d3,
    count(distinct if(day_7, user_id, null))/count(distinct user_id) retention_day_07,
    count(distinct if(day_7, user_id, null)) as dau_d7,
    count(distinct if(day_14, user_id, null))/count(distinct user_id) retention_day_14,
    count(distinct if(day_14, user_id, null)) as dau_d14,
    count(distinct if(day_28, user_id, null))/count(distinct user_id) retention_day_28,
    count(distinct if(day_28, user_id, null)) as dau_d28,
    count(distinct if(within_1, user_id, null))/count(distinct user_id) retention_within_01,
    count(distinct if(within_1, user_id, null)) as dau_w1,
    count(distinct if(within_3, user_id, null))/count(distinct user_id) retention_within_03,
    count(distinct if(within_3, user_id, null)) as dau_w3,
    count(distinct if(within_7, user_id, null))/count(distinct user_id) retention_within_07,
    count(distinct if(within_7, user_id, null)) as dau_w7,
    count(distinct if(within_14, user_id, null))/count(distinct user_id) retention_within_14,
    count(distinct if(within_14, user_id, null)) as dau_w14,
    count(distinct if(within_28, user_id, null))/count(distinct user_id) retention_within_28,
    count(distinct if(within_28, user_id, null)) as dau_w28,
  from getdayxflags
  group by all
)

, processnulls as (
  -- Null out any horizon that hasn't had a chance to mature within [startdate, enddate]:
  -- e.g. day_28 needs dt + 28 <= enddate to have been fully observable in the queried window.
  select
  dt,
  --platform,
  dau,
  if(date_add(dt, interval 1 day) <= enddate, retention_day_01, null) returnrate_day_01,
  dau_d1,
  if(date_add(dt, interval 3 day) <= enddate, retention_day_03, null) returnrate_day_03,
  dau_d3,
  if(date_add(dt, interval 7 day) <= enddate, retention_day_07, null) returnrate_day_07,
  dau_d7,
  if(date_add(dt, interval 14 day) <= enddate, retention_day_14, null) returnrate_day_14,
  dau_d14,
  if(date_add(dt, interval 28 day) <= enddate, retention_day_28, null) returnrate_day_28,
  dau_d28,
  if(date_add(dt, interval 1 day) <= enddate, retention_within_01, null) returnrate_within_01,
  dau_w1,
  if(date_add(dt, interval 3 day) <= enddate, retention_within_03, null) returnrate_within_03,
  dau_w3,
  if(date_add(dt, interval 7 day) <= enddate, retention_within_07, null) returnrate_within_07,
  dau_w7,
  if(date_add(dt, interval 14 day) <= enddate, retention_within_14, null) returnrate_within_14,
  dau_w14,
  if(date_add(dt, interval 28 day) <= enddate, retention_within_28, null) returnrate_within_28,
  dau_w28,
  from calculatereturnrate
)

select * 
from processnulls
where 1=1
