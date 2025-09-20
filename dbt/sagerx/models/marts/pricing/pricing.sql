-- pricing.sql

with

nadac as (

    select
        *
    from {{ ref('int_nadac_pricing') }}

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

pack_size as (

    select
        ndc11,
        total_product as pack_size,
        innermost_unit as unit_of_measure,
        outermost_unit as saleable_unit,
        packagedescription as fda_package_description
    from {{ ref('pack_size') }}

),

pricing as (

    select
        all_ndcs.*,
        nadac.product_name,
        nadac.nadac_per_unit,
        nadac.nadac_per_unit * pack_size.pack_size as nadac_per_pack,
        mccpd.unit_billing_price as mccpd_per_billing_unit,
        mccpd.unit_price as mccpd_per_unit,
        replace(mccpd.unit_billing_price, '$', '')::numeric * pack_size.pack_size as mccpd_per_billing_pack,
        replace(mccpd.unit_price, '$', '')::numeric * pack_size.pack_size as mccpd_per_pack,
        pack_size.pack_size,
        pack_size.unit_of_measure,
        pack_size.saleable_unit,
        pack_size.fda_package_description,
        nadac.nadac_description,
        mccpd.medication_name as mccpd_description
    from all_ndcs
    left join nadac
        on nadac.ndc = all_ndcs.ndc
    left join mccpd
        on mccpd.ndc = all_ndcs.ndc
    left join pack_size
        on pack_size.ndc11 = all_ndcs.ndc

)

select * from pricing
