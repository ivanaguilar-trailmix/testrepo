DECLARE days INT64 DEFAULT {lookback_days};
DECLARE start_date DATE DEFAULT cast('{start_date}' as date);
DECLARE end_date DATE DEFAULT cast('{end_date}' as date);

  select 
    dt,
    install_dt,
    date_trunc(install_dt,WEEK) as install_wk,
    days_since_install,
    user_id
  from trailmixgames-game-1.merger_prod_fact.fact_dsi_user_activity_cumulative
  where 1=1
  and dt between start_date and end_date
  and active=1
  group by all
  order by 1,2
