{{ config(tags = ['quality_gate', 'test']) }}
-- Singular test: Verify grain uniqueness of (date, category_l1) in mart_daily_summary
select
    date,
    category_l1,
    count(*) as row_cnt
from {{ ref('mart_daily_summary') }}
group by date, category_l1
having count(*) > 1
