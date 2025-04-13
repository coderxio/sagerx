-- int_nadac_current_pricing.sql

with

current_pricing as (

    select
        *
    from {{ ref('int_nadac_all_pricing') }}
    where is_last_price
        and start_date >= current_date - interval '1 year'
    order by ndc_description

)

select * from current_pricing
