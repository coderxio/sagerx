-- int_rxnorm_ndcs_to_products.sql

with

ndc as (

    select * from {{ ref('stg_rxnorm__ndcs') }}

),

clinical_product as (

    select * from {{ ref('stg_rxnorm__clinical_products') }}

),

brand_product as (

    select * from {{ ref('stg_rxnorm__brand_products') }}

)

select distinct
    ndc
    , coalesce(brand_product.rxcui, clinical_product.rxcui, null) as product_rxcui
    , coalesce(brand_product.name, clinical_product.name, null) as product_name
    , coalesce(brand_product.tty, clinical_product.tty, null) as product_tty
    , clinical_product.rxcui as clinical_product_rxcui
    , clinical_product.name as clinical_product_name
    , clinical_product.tty as clinical_product_tty
from ndc
left join clinical_product 
    on ndc.clinical_product_rxcui = clinical_product.rxcui
left join brand_product
    on ndc.brand_product_rxcui = brand_product.rxcui
