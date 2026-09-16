{{ config(tags = ['s3_dependent', 'integration']) }}

-- Singular test: Verify fact_events preserves the exact row count of int_events_enriched
with enriched_count as (

    select count(*) as cnt from {{ ref('int_events_enriched') }}

),

fact_count as (

    select count(*) as cnt from {{ ref('fact_events') }}

)

select
    enriched_count.cnt as enriched_rows,
    fact_count.cnt as fact_rows,
    abs(enriched_count.cnt - fact_count.cnt) as row_diff
from enriched_count, fact_count
where enriched_count.cnt != fact_count.cnt
