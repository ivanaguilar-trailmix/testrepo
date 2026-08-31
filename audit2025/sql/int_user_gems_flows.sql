select *
from kinetic-mile-245716.merger_prod_intermediate.int_user_gem_flows
where 1=1
and playfab_ts between '{start_date}' and '{end_date}'