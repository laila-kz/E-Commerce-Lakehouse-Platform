{{ config(tags = ['quality_gate', 'test']) }}
-- Singular test: Assert that no two versions of the same product have overlapping [valid_from, valid_to) validity windows
with versions as (

    select
        product_key,
        product_id,
        valid_from,
        coalesce(valid_to, cast('9999-12-31 23:59:59' as timestamp)) as valid_to
    from {{ ref('dim_product') }}

)

select
    a.product_id,
    a.product_key as version_a_key,
    a.valid_from as version_a_from,
    a.valid_to as version_a_to,
    b.product_key as version_b_key,
    b.valid_from as version_b_from,
    b.valid_to as version_b_to
from versions a
inner join versions b
    on a.product_id = b.product_id
   and a.product_key < b.product_key
where a.valid_from < b.valid_to
  and b.valid_from < a.valid_to
