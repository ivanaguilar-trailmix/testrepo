-- UIScreen

select 
regexp_extract(event_definition_id, r'^[^-]*') as  feature,
#event_definition_id,
context,
#area,
reason,
action,
min(level) as min_level,
count(distinct user_id) as records
from trailmixgames-game-1.merger_prod_raw.raw_UIScreen
where 1=1
and cast(timestamp as date)>=current_date-90
#and regexp_extract(event_definition_id, r'^[^-]*') is not null
#and reason = 'auto'
and context = 'icon'
#and action = 'entry'
group by all
order by 1

select distinct 
event_definition_id,
context,
reason,
action
from trailmixgames-game-1.merger_prod_raw.raw_UIScreen
where 1=1
and cast(timestamp as date)>=current_date-90
#and regexp_extract(event_definition_id, r'^[^-]*') is not null
#and reason = 'auto'
#and context = 'icon'
#and action = 'entry'
order by 1

-- GeneratorBoost
select
context,
min(level) as min_level,
count(*) as records
from trailmixgames-game-1.merger_prod_raw.raw_GeneratorBoost
where 1=1
and cast(timestamp as date)>=current_date-1
#and regexp_extract(event_definition_id, r'^[^-]*') is not null
#and reason = 'auto'
#and context = 'start'
#and action = 'entry'
group by all
order by 1

select *
from trailmixgames-game-1.merger_prod_raw.raw_GeneratorBoost
where 1=1
and cast(timestamp as date)>=current_date-1
#and regexp_extract(event_definition_id, r'^[^-]*') is not null
#and reason = 'auto'
#and context = 'start'
#and action = 'entry'
order by 1

-- CustomerOrder
select
context,
type,
customer_slot_id,
min(level) as min_level,
count(*) as records
from trailmixgames-game-1.merger_prod_raw.raw_CustomerOrder
where 1=1
and cast(timestamp as date)>=current_date-14
#and regexp_extract(event_definition_id, r'^[^-]*') is not null
#and reason = 'auto'
#and context = 'strt'
#and action = 'entry'
and customer_slot_id like 'timed%'
group by all
order by min_level desc

-- LOpsMilestoneCompleted
select 
context,
CASE
  WHEN regexp_extract(event_definition_id, r'^[^-]*') like '%TargetedEvent%' THEN 'TargetedEvent'
  ELSE regexp_extract(event_definition_id, r'^[^-]*')
END as  feature,
milestone,
min(milestone) as min_milestone,
min(level) as min_level,
count(distinct user_id) as records
from trailmixgames-game-1.merger_prod_raw.raw_LOpsMilestoneComplete
where 1=1
and cast(timestamp as date)>=current_date-90
and (regexp_extract(event_definition_id, r'^[^-]*') in ('Seasonal','SeasonPass','TimedAlbum') or regexp_extract(event_definition_id, r'^[^-]*') like '%TargetedEvent%')
#and regexp_extract(event_definition_id, r'^[^-]*') is not null
#and reason = 'auto'
#and context = 'strt'
#and action = 'entry'
and (milestone<=1 or milestone is null)
#and context in ('phaseStart', 'start')
group by all
order by feature, records desc,milestone
