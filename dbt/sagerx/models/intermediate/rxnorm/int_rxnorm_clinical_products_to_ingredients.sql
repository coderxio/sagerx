-- int_rxnorm_clinical_products_to_ingredients.sql

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

)

select
    rcp.rxcui as clinical_product_rxcui
    , rcp.name as clinical_product_name
    , rcp.tty as clinical_product_tty
    , string_agg(rcpc.rxcui, ' | ') as clinical_product_component_rxcui
    , string_agg(rcpc.name, ' | ') as clinical_product_compnent_name
    , string_agg(rcpc.tty, ' | ') as clinical_product_component_tty
    , string_agg(rdf.rxcui, ' | ') as dose_form_rxcui
    , string_agg(rdf.name, ' | ') as dose_form_name
    , string_agg(rdf.tty, ' | ') as dose_form_tty
    , string_agg(ri.rxcui, ' | ') as ingredient_rxcui
    , string_agg(ri.name, ' | ') as ingredient_name
    , string_agg(ri.tty, ' | ') as ingredient_tty
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
group by
    rcp.rxcui
    , rcp.name
    , rcp.tty
    , rcp.active
    , rcp.prescribable        

