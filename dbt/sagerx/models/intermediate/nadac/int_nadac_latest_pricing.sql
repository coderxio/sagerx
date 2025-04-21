-- int_nadac_latest_pricing.sql

with

latest_pricing as (

    select
        *
    from {{ ref('int_nadac_all_pricing') }}
    where is_last_price
    order by ndc_description

)

select * from latest_pricing
