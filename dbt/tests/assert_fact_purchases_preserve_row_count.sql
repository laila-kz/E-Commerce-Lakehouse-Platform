{{ config(tags = ['s3_dependent', 'integration']) }}

-- Singular test: Verify fact_purchases row count equals purchase events in int_events_enriched
with purchase_count as (

    select count(*) as cnt
    from {{ ref('int_events_enriched') }}
    where event_type = 'purchase'

),

fact_count as (

    select count(*) as cnt from {{ ref('fact_purchases') }}

)

select
    purchase_count.cnt as source_purchase_rows,
    fact_count.cnt as fact_rows,
    abs(purchase_count.cnt - fact_count.cnt) as row_diff
from purchase_count, fact_count
where purchase_count.cnt != fact_count.cnt
