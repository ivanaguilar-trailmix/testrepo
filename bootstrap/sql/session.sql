DECLARE start_date DATE DEFAULT cast('{start_date}' as date);

select
    dt,
    d.platform,
    a.user_id,
    coalesce(v.build_version, 'unknown') as build_version,
    count(distinct a.user_id) as count_distinct_users,
    count(distinct if(minutes_played>360,a.user_id,null)) as count_distinct_users_capped, 
    sum(n_sessions_active) as sum_sessions_active,
    sum(minutes_played) as sum_minutes_played,
    sum(if(minutes_played>360,360,minutes_played)) as sum_minutes_played_capped
from trailmixgames-game-1.merger_prod_fact.fact_dt_user_activity a
join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device d using (user_id)
left join trailmixgames-game-1.merger_prod_dimensions.dimchange_user_build v on (
    a.user_id = v.user_id and dt >= v.effective_ts and (dt < v.expiry_ts or v.expiry_ts is null) )
where 1=1
and dt>= start_date
and dt< current_date()
and d.platform!='WindowsEditor'
group by all
order by 1