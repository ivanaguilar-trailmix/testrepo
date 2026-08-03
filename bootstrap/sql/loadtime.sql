DECLARE start_date DATE DEFAULT cast('{start_date}' as date);

select
    --timestamp,
    CAST(timestamp AS DATE) as timestamp_date,
    coalesce(v.build_version, 'unknown') as build_version,
    a.session_id,
    a.user_id,
    d.platform as platform,
    g.install_country_code as country_code,
    a.context,
    time/1000 as seconds,
from trailmixgames-game-1.merger_prod_raw.raw_LoadTime a
join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device d using (user_id)
join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_geo g using (user_id)
left join trailmixgames-game-1.merger_prod_dimensions.dimchange_user_build v on (
    a.user_id = v.user_id and cast(timestamp as date) >= v.effective_ts and (cast(timestamp as date) < v.expiry_ts or v.expiry_ts is null) )
where 1=1
    and CAST(Timestamp AS DATE) >= start_date
    and CAST(Timestamp AS DATE) < current_date()