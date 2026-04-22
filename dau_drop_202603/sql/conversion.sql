DECLARE lookback INT64 DEFAULT {lookback};

with activity as (
  select distinct
    user_id
  from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
  where 1=1
  and dt>=current_date-lookback
  #and active=1
)

, installs as (
select
  user_id,
  platform,
  cast(install_ts as date) as install_dt,
from trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session
join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device using (user_id)
--join activity using (user_id)
where 1=1
and cast(install_ts as date)>=current_date-lookback
)

, converters as (
select
  user_id,
  install_dt,
  first_purchase_dt,
  date_diff(first_purchase_dt,install_dt, DAY) as days_to_first_purchase
from installs
join trailmixgames-game-1.merger_prod_dimensions.dim_user_first_purchase using (user_id)
where 1=1
and first_purchase_dt is not null
)


--select 
  --install_dt,
  --#platform,
  --count(distinct user_id) as total_users,
  --count(distinct user_id_d1) as converters_d1
--from (
  select 
    install_dt,
    t.user_id,
    platform,
    days_to_first_purchase,
    if(days_to_first_purchase=0, c.user_id,null) as user_id_d0,
    if(days_to_first_purchase=1, c.user_id,null) as user_id_d1,
    if(days_to_first_purchase=3, c.user_id,null) as user_id_d3,
    if(days_to_first_purchase=7, c.user_id,null) as user_id_d7,
    if(days_to_first_purchase=14, c.user_id,null) as user_id_d14,
    if(days_to_first_purchase=28, c.user_id,null) as user_id_d28
  from installs t
  left join converters c using(user_id, install_dt)
  --)
--group by all
--order by 1

  