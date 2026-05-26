DECLARE start_date DATE DEFAULT cast('{start_date}' as date);

select
timestamp,
server_timestamp, 
session_id,
payload_timestamp, 
CAST(SUBSTR(payload_timestamp, 1, 10) AS DATE) as payload_date,
user_id,
tutorial_step_id,
context,
time/1000 as seconds,
is_first_session,
row_number() over (partition by session_id order by payload_timestamp) as sequence
from trailmixgames-game-1.merger_prod_raw.raw_BootstrapStep
where 1=1
and CAST(SUBSTR(payload_timestamp, 1, 10) AS DATE) >= start_date
and CAST(SUBSTR(payload_timestamp, 1, 10) AS DATE) < current_date()
and context='cmpt'
order by user_id, payload_timestamp, context