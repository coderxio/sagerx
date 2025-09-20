-- int_nadac_pricing.sql

with

pricing as (

    select
        *
    from {{ ref('int_nadac_historical_pricing') }}
    where is_last_price
    order by nadac_description

)

select * from pricing
