-- Daily new installs by platform
-- Used to derive the age distribution of the existing user base at the forecast anchor date
-- Pull from game launch (2021-01-01) so veteran cohorts are represented

SELECT
    i.install_dt          AS dt,
    i.platform            AS platform,
    COUNT(DISTINCT i.user_id) AS new_installs
FROM trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device i
WHERE i.install_dt >= '2021-01-01'
  AND i.install_dt  < CURRENT_DATE()
  AND i.platform IN ('AND', 'IOS')
GROUP BY ALL
ORDER BY 1, 2
