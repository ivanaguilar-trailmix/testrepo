DECLARE start_date1 DATE DEFAULT cast('{start_date1}' as date);
DECLARE end_date1 DATE DEFAULT cast('{end_date1}' as date);
DECLARE start_date2 DATE DEFAULT cast('{start_date2}' as date);
DECLARE end_date2 DATE DEFAULT cast('{end_date2}' as date);

select * 
from trailmixgames-game-1.merger_prod_ab.ab_dt_user_active_metrics
where 1=1
and install_dt>=start_date1
and install_dt<=end_date1
and dt>=start_date1
and dt<=end_date1
union all
select *
from trailmixgames-game-1.merger_prod_ab.ab_dt_user_active_metrics
where 1=1
and install_dt>=start_date2
and install_dt<=end_date2
and dt>=start_date2
and dt<=end_date2