DECLARE assignmentstartdate DATE DEFAULT cast('{assignmentstartdate}' as date);
DECLARE lookback INT64 DEFAULT {lookback};

select
m.*
from trailmixgames-game-1.merger_prod_ab.ab_dt_user_active_metrics m
left join trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude u
    on m.user_id = u.user_id
where 1=1
and m.dt>=assignmentstartdate-lookback
and u.user_id is null