-- pricing_historical.sql

with

nadac_historical as (

    select
        *
    from {{ ref('int_nadac_historical_pricing') }}

),

mccpd as (

    select
        *
    -- TODO: make a stanging table and int table instead of hitting source in a mart
    from {{ source('mccpd', 'mccpd') }}
),

all_ndcs as (

    select ndc from nadac_historical

    union

    select ndc from mccpd

),

pricing as (

    select
        all_ndcs.*,
        nadac_historical.ndc_description as nadac_description,
        nadac_historical.nadac_per_unit,
        mccpd.medication_name as mccpd_description,
        mccpd.unit_billing_price,
        mccpd.unit_price
    from all_ndcs
    left join nadac_historical
        on nadac_historical.ndc = all_ndcs.ndc
    left join mccpd
        on mccpd.ndc = all_ndcs.ndc

)

select * from pricing
