DECLARE start_date DATE DEFAULT CAST('{start_date}' AS DATE);

WITH activity AS (
  SELECT dt, user_id
  FROM trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity
  WHERE dt >= start_date
),

installs AS (
  SELECT DISTINCT
    user_id,
    CAST(install_ts AS DATE) AS install_dt,
    d.platform,
    ua.display_campaign_network
  FROM activity
  JOIN trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device d USING (user_id)
  JOIN trailmixgames-game-1.merger_prod_dimensions.dim_user_install_session i USING (user_id)
  join trailmixgames-game-1.merger_prod_dimensions.dimchange_user_install_ua ua USING (user_id)
  WHERE CAST(install_ts AS DATE) = dt
    AND CAST(install_ts AS DATE) >= start_date
)

select *
from installs
where 1=1