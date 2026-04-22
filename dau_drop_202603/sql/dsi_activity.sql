DECLARE days INT64 DEFAULT {lookback_days};

  select 
    dt,
    install_dt,
    date_trunc(install_dt,WEEK) as install_wk,
    days_since_install,
    user_id
  from trailmixgames-game-1.merger_prod_fact.fact_dsi_user_activity_cumulative
  where 1=1
  and dt>=current_date-days
  and active=1
  group by all
  order by 1,2
