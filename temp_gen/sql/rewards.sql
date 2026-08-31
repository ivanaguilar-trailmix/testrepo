with users_to_exclude as (
select distinct
user_id
from trailmixgames-game-1.merger_prod_dimensions.dim_users_to_exclude
where 1=1
)



select #distinct context
cast(event_ts as date) event_dt,
rc.user_id,
context,
reason,
reward_id,
reward_instance_id,
#t1.count as t1_count,
#t1.id as t1_id,
#t2.count as t2_count,
#t2.id as t2_id,
count(distinct rc.user_id) as users
FROM trailmixgames-game-1.merger_prod_raw_processing.rp_RewardClaimed rc
LEFT JOIN users_to_exclude u ON rc.user_id = u.user_id,
#UNNEST(dynamic_rewards_item_array) AS t0
UNNEST(wallet_diff_array) as t1
#UNNEST(wallet_balance_array) as t2
where 1=1
and context = 'lop_GeneratorEvent'
and reward_instance_id is not null
and reward_instance_id!=''
and t1.id like '%liveops-GeneratorEvent-GardenGlory%'
and event_ts>= '2026-08-17'
and u.user_id IS NULL
group by all
order by 1