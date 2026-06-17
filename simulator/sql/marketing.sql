DECLARE start_date DATE DEFAULT cast('{start_date}' as date);
DECLARE end_date DATE DEFAULT cast('{end_date}' as date);

select 
  dt,
  display_platform_name,
  sum(usd_cost) as usd_cost,
  sum(n_playfab_installs) as installs,
  safe_divide(sum(usd_cost),sum(n_playfab_installs)) as cpi
from trailmixgames-game-1.merger_prod_viz.viz_cohort_marketing_metrics
where 1=1
and dt>=start_date
and dt<=end_date-1
and display_platform_name is not null
group by all 
order by 1,2