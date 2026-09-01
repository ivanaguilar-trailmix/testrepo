WITH latest AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_ts DESC) AS rn
  FROM `trailmixgames-game-1.merger_prod_raw_processing.rpp_lighthouse_products`
  WHERE product_category = "daily_offers"
)
SELECT
  product_id,
  price,
  (SELECT SUM(cnt) FROM UNNEST(product_contents_array)) AS total_contents_cnt,
  (SELECT SUM(cnt) FROM UNNEST(product_bonus_array)) AS total_bonus_cnt
FROM latest
WHERE rn = 1
