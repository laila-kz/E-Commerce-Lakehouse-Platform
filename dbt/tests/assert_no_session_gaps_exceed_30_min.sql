-- Singular test: Assert that no event-to-event gap within the same session exceeds 30 minutes (1800 seconds)
-- We use LAG partitioned by session_id (not user_id) to only check intra-session gaps
with session_events as (
    select
        session_id,
        user_id,
        event_key,
        event_time,
        lag(event_time) over (
            partition by session_id
            order by event_time, event_key
        ) as prev_event_in_session
    from {{ ref('fact_events') }}
)

select
    session_id,
    user_id,
    event_key,
    event_time,
    prev_event_in_session,
    (epoch(event_time) - epoch(prev_event_in_session)) as gap_seconds
from session_events
where prev_event_in_session is not null
  and (epoch(event_time) - epoch(prev_event_in_session)) > 1800
