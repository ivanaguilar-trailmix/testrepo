

SELECT
  user_id,
  dt,
  product_id,
  product_type,
  product_theme,
  iap_price_usd AS price_point_usd,
  product_price_group,
  MIN(dt) AS first_seen,
  MAX(dt) AS last_seen,
  SUM(n_trans) AS n_trans,
  ROUND(SUM(usd_iap_revenue), 2) AS usd_revenue
FROM `trailmixgames-game-1.merger_prod_fact.fact_dt_user_product_iap_revenue`
WHERE dt >= "2026-08-17"
  AND product_category = "daily_offers"
  --AND product_id LIKE "%2026Q3%"
GROUP BY user_id, dt, product_id, product_type, product_theme, iap_price_usd, product_price_group
--HAVING MIN(dt) = "2026-08-26"
ORDER BY usd_revenue DESC