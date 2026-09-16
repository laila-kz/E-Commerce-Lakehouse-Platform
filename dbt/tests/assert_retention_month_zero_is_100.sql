{{ config(tags = ['quality_gate', 'test']) }}
-- Singular test: Verify retention rate at month 0 is exactly 1.0 (100%) for all cohorts
select
    cohort_month,
    months_since_first_purchase,
    cohort_size,
    retained_count,
    retention_rate
from {{ ref('mart_customer_retention') }}
where months_since_first_purchase = 0
  and retention_rate != 1.0
