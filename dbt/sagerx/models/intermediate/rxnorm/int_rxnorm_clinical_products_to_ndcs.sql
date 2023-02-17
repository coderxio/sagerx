-- int_rxnorm_clinical_products_to_ndcs.sql

with

rcp as (

    select * from {{ ref('stg_rxnorm__clinical_products') }}

),

rcpcl as (

    select * from {{ ref('stg_rxnorm__clinical_product_component_links') }}

),

rcpc as (

    select * from {{ ref('stg_rxnorm__clinical_product_components') }}

),

rdf as (

    select * from {{ ref('stg_rxnorm__dose_forms') }}

),

ri as (

    select * from {{ ref('stg_rxnorm__ingredients') }}

),

rn as (

    select * from {{ ref('stg_rxnorm__ndcs') }}

)

select
    rcp.rxcui as clinical_product_rxcui
    , rcp.name as clinical_product_name
    , rcp.tty as clinical_product_tty
    , rcpc.rxcui as clinical_product_component_rxcui
    , rcpc.name as clinical_product_compnent_name
    , rcpc.tty as clinical_product_component_tty
    , rdf.rxcui as dose_form_rxcui
    , rdf.name as dose_form_name
    , rdf.tty as dose_form_tty
    , ri.rxcui as ingredient_rxcui
    , ri.name as ingredient_name
    , ri.tty as ingredient_tty
    , rn.brand_product_rxcui
    , rn.ndc
    , rcp.active
    , rcp.prescribable
from rcp 
left join rcpcl 
    on rcp.rxcui = rcpcl.clinical_product_rxcui 
left join rcpc 
    on rcpcl.clinical_product_component_rxcui = rcpc.rxcui 
left join rdf 
    on rcpc.dose_form_rxcui = rdf.rxcui 
left join ri 
    on rcpc.ingredient_rxcui = ri.rxcui 
left join rn
    on rcp.rxcui = rn.clinical_product_rxcui
