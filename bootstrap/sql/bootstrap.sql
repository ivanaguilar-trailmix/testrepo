DECLARE start_date DATE DEFAULT cast('{start_date}' as date);

select
    timestamp,
    CAST(timestamp AS DATE) as timestamp_date,
    server_timestamp, 
    session_id,
    payload_timestamp, 
    CAST(SUBSTR(payload_timestamp, 1, 10) AS DATE) as payload_date,
    user_id,
    d.platform as platform,
    g.install_country_code as country_code,
    tutorial_step_id,
    context,
    time/1000 as seconds,
    is_first_session,
    row_number() over (partition by session_id order by payload_timestamp) as sequence
from trailmixgames-game-1.merger_prod_raw.raw_BootstrapStep
join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device d using (user_id)
join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_geo g using (user_id)
where 1=1
    and CAST(Timestamp AS DATE) >= start_date
    and CAST(Timestamp AS DATE) < current_date()
    --and context='cmpt'
order by user_id, payload_timestamp, context