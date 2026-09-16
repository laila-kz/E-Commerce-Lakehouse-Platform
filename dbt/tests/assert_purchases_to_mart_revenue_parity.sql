{{ config(tags = ['offline_ready', 'parity']) }}

-- Singular test: Assert exact monetary revenue parity between fact_purchases and mart_daily_summary
-- Fails if the aggregate revenue drift between the purchase-grain fact table and the daily mart exceeds $0.01

with fact_rev as (
    select coalesce(round(sum(revenue), 2), 0.0) as total_fact_revenue
    from {{ ref('fact_purchases') }}
),

mart_rev as (
    select coalesce(round(sum(total_revenue), 2), 0.0) as total_mart_revenue
    from {{ ref('mart_daily_summary') }}
),

drift_calculation as (
    select
        f.total_fact_revenue,
        m.total_mart_revenue,
        round(abs(f.total_fact_revenue - m.total_mart_revenue), 2) as revenue_drift
    from fact_rev f
    cross join mart_rev m
)

select *
from drift_calculation
where revenue_drift > 0.01
