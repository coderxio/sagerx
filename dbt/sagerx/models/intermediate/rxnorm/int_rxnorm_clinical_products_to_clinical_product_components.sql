-- int_rxnorm_clinical_products_to_clinical_product_components.sql

with

ndc as (

    select * from {{ ref('stg_rxnorm__ndcs') }}

),

rcp as (

    select * from {{ ref('stg_rxnorm__clinical_products') }}

),

rcpcl as (

    select * from {{ ref('stg_rxnorm__clinical_product_component_links') }}

),

rcpc as (

    select * from {{ ref('stg_rxnorm__clinical_product_components') }}

)

select
    rcp.rxcui as clinical_product_rxcui
    , rcp.name as clinical_product_name
    , rcp.tty as clinical_product_tty
    , rcpc.rxcui as clinical_product_component_rxcui
    , rcpc.name as clinical_product_compnent_name
    , rcpc.tty as clinical_product_component_tty
    , rcp.active
    , rcp.prescribable
from rcp 
left join rcpcl 
    on rcp.rxcui = rcpcl.clinical_product_rxcui 
left join rcpc 
    on rcpcl.clinical_product_component_rxcui = rcpc.rxcui 
