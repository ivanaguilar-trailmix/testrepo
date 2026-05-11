-- Daily Active Users per platform 
-- Used to anchor the forecast start point

DECLARE start_date DATE DEFAULT cast('{start_date}' as date);


-- AD REV
WITH ad_rev AS (
  SELECT 
    user_id, 
    dt, 
    usd_ad_revenue_est / 0.85 AS gross_usd_ad_revenue,
    usd_ad_revenue_est AS net_usd_ad_revenue
  FROM trailmixgames-game-1.merger_prod_fact.fact_dt_user_ad_revenue
  WHERE 1=1
  and dt>=start_date
)

-- IAP REV
, iap_rev AS (
  SELECT 
    user_id, 
    dt, 
    usd_iap_revenue AS gross_usd_iap_revenue, 
    usd_net_iap_revenue AS net_usd_iap_revenue,
    1 AS payer_flag
  FROM trailmixgames-game-1.merger_prod_fact.fact_dt_user_iap_revenue
  WHERE 1=1
  and dt>=start_date
)

  select 
    dt,
    platform,
    count(distinct user_id) as dau,
    sum(gross_usd_iap_revenue) as iap_revenue,
    sum(gross_usd_ad_revenue) as ad_revenue
  from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device using (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session using (user_id)
  left join ad_rev using (user_id, dt)
  left join iap_rev using (user_id, dt)
  where 1=1
  and dt >= start_date
  and dt < CURRENT_DATE()
  and platform in ('AND', 'IOS')
  group by all
  order by 1,2 
