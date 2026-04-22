DECLARE abteststartdate DATE DEFAULT cast('{abteststartdate}' as date);
DECLARE lookback INT64 DEFAULT {lookback};

select 
*
from trailmixgames-game-1.merger_prod_ab.ab_dt_user_active_metrics
where 1=1
and dt>=abteststartdate-lookback