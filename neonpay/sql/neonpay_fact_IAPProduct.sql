select
*
from trailmixgames-game-1.merger_prod_fact.fact_dt_user_product_iap_revenue
where 1=1
and dt >= '2026-01-01'
and transaction_store = 'NeonPay'