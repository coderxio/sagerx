-- stg_rxnorm__products.sql

with

rcp as (

    select * from {{ ref('stg_rxnorm__clinical_products') }}

),

rbp as (

    select * from {{ ref('stg_rxnorm__brand_products') }}

)

select distinct
    coalesce(rbp.rxcui, rcp.rxcui, null) as product_rxcui
    , coalesce(rbp.name, rcp.name, null) as product_name
    , coalesce(rbp.tty, rcp.tty, null) as product_tty
    , rcp.rxcui as clinical_product_rxcui
    , rcp.name as clinical_product_name
    , rcp.tty as clinical_product_tty
from rcp
left join rbp
    on rbp.clinical_product_rxcui = rcp.rxcui
