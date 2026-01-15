select ef.user_id 
         , ef.dt
         , 'energy' as currency
         , COALESCE(ef.flow_reason_category, 'unknown') AS flow_reason_category
         , COALESCE(IF(lower(ef.flow_reason) in ('unknown', ''), 'unknown', ef.flow_reason), 'unknown') AS flow_reason
         , SUM(IF(ef.flow_reason_category not in ('iap', 'ads'), ef.inflow, 0)) AS n_free_economy_inflow
         , SUM(IF(ef.flow_reason_category = 'iap', ef.inflow, 0)) AS n_paid_economy_inflow
         , SUM(IF(ef.flow_reason_category = 'ads', ef.inflow, 0)) AS n_ads_economy_inflow
         , -1*SUM(COALESCE(ef.outflow,0)) AS n_economy_outflow
from trailmixgames-game-1.merger_prod_fact.fact_dt_user_reason_detail_energy_flows ef
where 1=1 
and ef.dt = current_date - {period_offset}
and ef.flow_reason != 'install_reward'
group by all