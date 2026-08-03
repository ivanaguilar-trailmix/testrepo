select
*
from trailmixgames-game-1.merger_prod_fact.fact_dt_user_dtc_purchase_rates
where 1=1
and dt >= '2026-01-01'
and purchased_dtc=1