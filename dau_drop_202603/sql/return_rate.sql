DECLARE lookback INT64 DEFAULT {lookback};

with activity as (
select 
  user_id,
  dt,
  platform,
from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device using (user_id)
where 1=1
and dt>current_date-lookback
and platform in ('AND','IOS')
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
      date_add(dt, interval 1 day) in unnest(nextval) day_1,
      date_add(dt, interval 3 day) in unnest(nextval) day_3,
      date_add(dt, interval 7 day) in unnest(nextval) day_7,
      date_add(dt, interval 14 day) in unnest(nextval) day_14,
      date_add(dt, interval 28 day) in unnest(nextval) day_28
    from arraydates
)

select *
from getdayxflags

--, calculatereturnrate as (
  --select 
    --dt,
    ----platform,
    --count(distinct user_id) dau,
    --count(distinct if(day_1, user_id, null))/count(distinct user_id) retention_day_01,
    --count(distinct if(day_1, user_id, null)) as dau_d1,
    --count(distinct if(day_3, user_id, null))/count(distinct user_id) retention_day_03,
    --count(distinct if(day_3, user_id, null)) as dau_d3,
    --count(distinct if(day_7, user_id, null))/count(distinct user_id) retention_day_07,
    --count(distinct if(day_7, user_id, null)) as dau_d7,
    --count(distinct if(day_14, user_id, null))/count(distinct user_id) retention_day_14,
    --count(distinct if(day_14, user_id, null)) as dau_d14,
    --count(distinct if(day_28, user_id, null))/count(distinct user_id) retention_day_28,
    --count(distinct if(day_28, user_id, null)) as dau_d28,
  --from getdayxflags
  --group by all
--)
--
--, processnulls as (
  --select 
  --dt,
  ----platform,
  --dau,
  --if(retention_day_01 > 0 and retention_day_01 < 1, retention_day_01, null) returnrate_day_01,
  --dau_d1,
  --if(retention_day_03 > 0 and retention_day_03 < 1, retention_day_03, null) returnrate_day_03,
  --dau_d3,
  --if(retention_day_07 > 0 and retention_day_07 < 1, retention_day_07, null) returnrate_day_07,
  --dau_d7,
  --if(retention_day_14 > 0 and retention_day_14 < 1, retention_day_14, null) returnrate_day_14,
  --dau_d14,
  --if(retention_day_28 > 0 and retention_day_28 < 1, retention_day_28, null) returnrate_day_28,
  --dau_d28,
  --from calculatereturnrate
--)
--
--select * 
--from processnulls
--where 1=1
