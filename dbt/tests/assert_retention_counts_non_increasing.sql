{{ config(tags = ['quality_gate', 'test']) }}
-- Singular test: Verify retained_count never exceeds original cohort_size
select
    cohort_month,
    months_since_first_purchase,
    cohort_size,
    retained_count
from {{ ref('mart_customer_retention') }}
where retained_count > cohort_size
