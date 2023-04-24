-- int_rxnorm_clinical_products_to_ingredient_strengths.sql

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

ricl as (

    select * from {{ ref('stg_rxnorm__ingredient_component_links') }}

),

ric as (

    select * from {{ ref('stg_rxnorm__ingredient_components') }}

),

risl as (

    select * from {{ ref('stg_rxnorm__ingredient_strength_links') }}

),

ris as (

    select * from {{ ref('stg_rxnorm__ingredient_strengths') }}

),

pinl as (

    select * from {{ ref('stg_rxnorm__precise_ingredient_links') }}

),

pin as (

    select * from {{ ref('stg_rxnorm__precise_ingredients') }}

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
    , ric.rxcui as ingredient_component_rxcui
    , ric.name as ingredient_component_name
    , ric.tty as ingredient_component_tty
    , ris.rxcui as ingredient_strength_rxcui
    , ris.name as ingredient_strength_name
    , ris.numerator_value as strength_numerator_value
    , ris.numerator_unit as strength_numerator_unit
    , ris.denominator_value as strength_denominator_value
    , ris.denominator_unit as strength_denominator_unit
    , ris.text as strength_text
    , pin.rxcui as precise_ingredient_rxcui
    , pin.name as precise_ingredient_name
    , pin.tty as precise_ingredient_tty
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
left join ricl 
    on ri.rxcui = ricl.ingredient_rxcui 
left join ric 
    on ricl.ingredient_component_rxcui = ric.rxcui 
left join risl 
    on rcpc.rxcui = risl.clinical_product_component_rxcui 
    and ric.rxcui = risl.ingredient_component_rxcui 
left join ris 
    on risl.ingredient_strength_rxcui = ris.rxcui
left join pinl
    on ris.rxcui = pinl.ingredient_strength_rxcui
left join pin
    on pinl.precise_ingredient_rxcui = pin.rxcui
