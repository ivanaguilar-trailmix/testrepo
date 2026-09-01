SELECT
  user_id,
  DATE(event_ts) AS dt,
  product_id,
  COUNT(*) AS n_starts
FROM `trailmixgames-game-1.merger_prod_raw_processing.rp_IAPStart`
WHERE DATE(event_ts) >= "2026-08-17"
  AND product_id LIKE "%DailyOffers%"
GROUP BY user_id, dt, product_id
ORDER BY n_starts DESC
