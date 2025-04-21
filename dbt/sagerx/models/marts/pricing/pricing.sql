-- pricing.sql

with

nadac as (

    select
        *
    from {{ ref('int_nadac_all_pricing') }}

),

mccpd as (

    select
        *
    -- TODO: make a stanging table and int table instead of hitting source in a mart
    from {{ source('mccpd', 'mccpd') }}
),

all_ndcs as (

    select ndc from nadac

    union

    select ndc from mccpd

),

pricing as (

    select
        all_ndcs.*,
        nadac.ndc_description as nadac_description,
        nadac.nadac_per_unit,
        mccpd.medication_name as mccpd_description,
        mccpd.unit_billing_price,
        mccpd.unit_price
    from all_ndcs
    left join nadac
        on nadac.ndc = all_ndcs.ndc
    left join mccpd
        on mccpd.ndc = all_ndcs.ndc

)

select * from pricing
