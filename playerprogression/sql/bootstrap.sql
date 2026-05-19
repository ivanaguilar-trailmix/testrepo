DECLARE start_date DATE DEFAULT cast('{start_date}' as date);

select 
session_id,
payload_timestamp, 
date(payload_timestamp) as payload_date,
user_id,
tutorial_step_id,
context,
time/1000 as seconds,
is_first_session,
row_number() over (partition by session_id order by payload_timestamp) as sequence
from trailmixgames-game-1.merger_prod_raw.raw_BootstrapStep
where 1=1
and date(payload_timestamp) >= start_date
--and context='cmpt'
order by user_id, payload_timestamp, context