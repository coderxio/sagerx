with brand_products as (
    select * from {{ ref('stg_rxnorm__brand_products') }}
)

, fda_ndcs as (
    select * from {{ ref('stg_fda_ndc__ndcs') }}
)

, rxnorm_ndcs_to_products as (
    select * from {{ ref('int_rxnorm_ndcs_to_products') }}
)

, map as (
    select
        prod.tty as product_tty
        , prod.rxcui as product_rxcui
        , prod.name as product_name
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
    order by prod.rxcui
)

select
    *
from map
