select
  event_ts,
  user_id, 
  purchase_id,
  transaction_id,
  transaction_store,
  outcome,
  iap_price_usd,
  currency_code,
  currency_amount
from trailmixgames-game-1.merger_prod_raw_processing.rp_IAPSuccess
where 1=1
and cast(event_ts as date) >= '2026-01-01'
and transaction_store = 'NeonPay'