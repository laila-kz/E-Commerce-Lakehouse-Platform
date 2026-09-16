{{ config(tags = ['quality_gate', 'test']) }}
-- Singular test: Verify grain uniqueness of (category_l1, period) in mart_category_performance
select
    category_l1,
    period,
    count(*) as row_cnt
from {{ ref('mart_category_performance') }}
group by category_l1, period
having count(*) > 1
