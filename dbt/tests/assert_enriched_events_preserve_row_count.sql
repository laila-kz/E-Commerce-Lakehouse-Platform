{{ config(tags = ['quality_gate', 'test']) }}
-- Singular test: Assert that int_events_enriched preserves the exact row count of stg_events
with stg_count as (

    select count(*) as cnt from {{ ref('stg_events') }}

),

enriched_count as (

    select count(*) as cnt from {{ ref('int_events_enriched') }}

)

select
    stg_count.cnt as stg_rows,
    enriched_count.cnt as enriched_rows,
    abs(stg_count.cnt - enriched_count.cnt) as row_diff
from stg_count, enriched_count
where stg_count.cnt != enriched_count.cnt
