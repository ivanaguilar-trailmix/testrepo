DECLARE start_date DATE DEFAULT cast('{start_date}' as DATE);

WITH economy_flows_daily as (
     SELECT gf.user_id 
         , gf.dt
         , 'gems' as currency
         , COALESCE(gf.flow_reason_category, 'unknown') AS flow_reason_category
         , COALESCE(IF(lower(gf.flow_reason) in ('unknown', ''), 'unknown', gf.flow_reason), 'unknown') AS flow_reason
         , SUM(IF(gf.flow_reason_category != 'iap', gf.inflow, 0)) AS n_free_economy_inflow
         , SUM(IF(gf.flow_reason_category = 'iap', gf.inflow, 0)) AS n_paid_economy_inflow
         , 0 AS n_ads_economy_inflow -- now player can't receive gems for ads
         , -1*SUM(COALESCE(gf.outflow,0)) AS n_economy_outflow
    FROM trailmixgames-game-1.merger_prod_fact.fact_dt_user_reason_detail_gem_flows gf
    LEFT JOIN trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude ute ON gf.user_id = ute.user_id     
    WHERE ute.user_id IS NULL
      AND gf.flow_reason != 'install_reward'  
      AND gf.dt >= start_date
    GROUP BY ALL
    UNION ALL
    SELECT cf.user_id 
         , cf.dt
         , 'coins' as currency
         , COALESCE(cf.flow_reason_category, 'unknown') AS flow_reason_category
         , COALESCE(IF(lower(cf.flow_reason) in ('unknown', ''), 'unknown', cf.flow_reason), 'unknown') AS flow_reason
         , SUM(cf.inflow) AS n_free_economy_inflow
         , 0 AS n_paid_economy_inflow
         , 0 AS n_ads_economy_inflow
         , -1*SUM(COALESCE(cf.outflow,0)) AS n_economy_outflow
    FROM trailmixgames-game-1.merger_prod_fact.fact_dt_user_reason_detail_coin_flows cf
    LEFT JOIN trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude ute ON cf.user_id = ute.user_id
    WHERE ute.user_id IS NULL
      AND cf.flow_reason != 'install_reward' 
      AND cf.dt >= start_date
    GROUP BY ALL
    UNION ALL   
    SELECT ef.user_id 
         , ef.dt
         , 'energy' as currency
         , COALESCE(ef.flow_reason_category, 'unknown') AS flow_reason_category
         , COALESCE(IF(lower(ef.flow_reason) in ('unknown', ''), 'unknown', ef.flow_reason), 'unknown') AS flow_reason
         , SUM(IF(ef.flow_reason_category not in ('iap', 'ads'), ef.inflow, 0)) AS n_free_economy_inflow
         , SUM(IF(ef.flow_reason_category = 'iap', ef.inflow, 0)) AS n_paid_economy_inflow
         , SUM(IF(ef.flow_reason_category = 'ads', ef.inflow, 0)) AS n_ads_economy_inflow
         , -1*SUM(COALESCE(ef.outflow,0)) AS n_economy_outflow

    FROM `trailmixgames-game-1.merger_prod_fact.fact_dt_user_reason_detail_energy_flows` ef
LEFT JOIN trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude ute using(user_id)
        
    WHERE 1=1
      AND ef.flow_reason != 'install_reward'
      AND ef.dt >= start_date

    GROUP BY ALL
)

, economy_balance_daily AS (
    SELECT gf.user_id 
         , gf.dt
         , gf.currency
         , 'All' AS flow_reason_category
         , 'All' AS flow_reason
         , gf.balance_start
         , gf.balance_end
    FROM `trailmixgames-game-1.merger_prod_fact.fact_dsi_user_economy_balance_snapshot` gf
    LEFT JOIN trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude ute using(user_id)
    WHERE 1=1
    AND gf.dt >= start_date
)

, currency_list as (
    --since we need different levels of aggregation in the final data model and tableau dashboard, adding these as unioned sets for each level of aggregation breakdown 
    SELECT 
        DISTINCT currency, flow_reason_category, flow_reason
    FROM economy_flows_daily

    UNION ALL

    SELECT 
        DISTINCT currency, flow_reason_category, 'All' AS flow_reason
    FROM economy_flows_daily

    UNION ALL

    SELECT 
        DISTINCT currency, 'All' AS flow_reason_category, 'All' AS flow_reason
    FROM economy_flows_daily
)

, economy_flows_daily_agg AS (

    SELECT user_id
         , dt
         , currency
         , flow_reason_category
         , flow_reason
         , SUM(COALESCE(n_free_economy_inflow, 0)) AS n_free_economy_inflow
         , SUM(COALESCE(n_paid_economy_inflow, 0)) AS n_paid_economy_inflow
         , SUM(COALESCE(n_ads_economy_inflow, 0)) AS n_ads_economy_inflow
         , SUM(COALESCE(n_economy_outflow, 0)) AS n_economy_outflow
    FROM economy_flows_daily
    GROUP BY ALL

    UNION ALL 

    SELECT user_id
         , dt
         , currency
         , flow_reason_category
         , 'All' AS flow_reason
         , SUM(COALESCE(n_free_economy_inflow, 0)) AS n_free_economy_inflow
         , SUM(COALESCE(n_paid_economy_inflow, 0)) AS n_paid_economy_inflow
         , SUM(COALESCE(n_ads_economy_inflow, 0)) AS n_ads_economy_inflow
         , SUM(COALESCE(n_economy_outflow, 0)) AS n_economy_outflow
    FROM economy_flows_daily
    GROUP BY ALL

    UNION ALL 

    SELECT user_id
         , dt
         , currency
         , 'All' AS flow_reason_category
         , 'All' AS flow_reason
         , SUM(COALESCE(n_free_economy_inflow, 0)) AS n_free_economy_inflow
         , SUM(COALESCE(n_paid_economy_inflow, 0)) AS n_paid_economy_inflow
         , SUM(COALESCE(n_ads_economy_inflow, 0)) AS n_ads_economy_inflow
         , SUM(COALESCE(n_economy_outflow, 0)) AS n_economy_outflow
    FROM economy_flows_daily
    GROUP BY ALL

)

select 
dt, 
user_id,
currency,
--flow_reason_category,
--flow_reason,
balance_start,
balance_end,
sum(n_free_economy_inflow + n_paid_economy_inflow + n_ads_economy_inflow) as n_economy_inflow,
sum(n_economy_outflow) as n_economy_outflow,
from economy_flows_daily
join economy_balance_daily using (dt, user_id, currency)
where 1=1
--and currency = 'gems'
group by all
order by 1