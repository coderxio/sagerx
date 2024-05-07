with brand_products as (
    select * from {{ ref('stg_rxnorm__products') }}
    where product_tty in ('SBD', 'BPCK') -- brand products only
)

, fda_ndcs as (
    select * from {{ ref('stg_fda_ndc__ndcs') }}
)

, rxnorm_ndcs_to_products as (
    select * from {{ ref('int_rxnorm_ndcs_to_products') }}
)

, map as (
    select
        prod.product_tty
        , prod.product_rxcui
        , prod.product_name
        , ndc.product_tty as ndc_product_tty
        , ndc.product_rxcui as ndc_product_rxcui
        , ndc.product_name as ndc_product_name
        , ndc.ndc
        , fda.product_startmarketingdate
        , fda.package_startmarketingdate
    from brand_products prod
    left join rxnorm_ndcs_to_products ndc
        on ndc.clinical_product_rxcui = prod.clinical_product_rxcui
    left join fda_ndcs fda
        on fda.ndc11 = ndc.ndc
    order by prod.product_rxcui
)

select
    *
from map
