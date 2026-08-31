DECLARE start_date DATE DEFAULT cast('{start_date}' as date);

select
    timestamp,
    CAST(timestamp AS DATE) as timestamp_date,
    server_timestamp, 
    session_id,
    payload_timestamp, 
    CAST(SUBSTR(payload_timestamp, 1, 10) AS DATE) as payload_date,
    user_id,
    d.platform as platform,
    g.install_country_code as country_code,
    tutorial_step_id,
    context,
    time/1000 as seconds,
    is_first_session,
    -- payload_timestamp is per-event and reliably orders steps within a session; `timestamp` only
    -- takes ~1 value per upload batch (~3 distinct values per ~9-row session) and can't do this on
    -- its own. This flags client clocks we can't trust: payload_timestamp should sit a couple
    -- seconds *before* timestamp (upload latency); negative or multi-hour+ gaps mean the device
    -- clock was wrong, not a legitimate delay (bootstrap events fire at install, not after being
    -- queued offline for hours).
    TIMESTAMP_DIFF(timestamp, CAST(payload_timestamp AS TIMESTAMP), SECOND) as clock_gap_seconds,
    -- Same clock check, pre-applied to payload_date: the query's own date filter (below) is on
    -- `timestamp`, so a row can pass the cohort window while payload_date is garbage (days/weeks/
    -- months off) if the device clock was wrong. Use this instead of payload_date for any
    -- date-bucketed/charted analysis; falls back to timestamp_date for the bad-clock rows.
    IF(
        TIMESTAMP_DIFF(timestamp, CAST(payload_timestamp AS TIMESTAMP), SECOND) NOT BETWEEN 0 AND 3600,
        CAST(timestamp AS DATE),
        CAST(SUBSTR(payload_timestamp, 1, 10) AS DATE)
    ) as payload_date_safe,
    row_number() over (partition by session_id order by payload_timestamp) as sequence
from trailmixgames-game-1.merger_prod_raw.raw_BootstrapStep
join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_device d using (user_id)
join trailmixgames-game-1.merger_prod_dimensions.dim_user_install_geo g using (user_id)
where 1=1
    and CAST(Timestamp AS DATE) >= start_date
    and CAST(Timestamp AS DATE) < current_date()
    and CAST(SUBSTR(payload_timestamp, 1, 10) AS DATE) >= start_date
    and CAST(SUBSTR(payload_timestamp, 1, 10) AS DATE) < current_date()
    --and context='cmpt'
order by user_id, payload_timestamp, context