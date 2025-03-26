-- int_umls_clinical_products_to_crosswalk_codes.sql

with

clinical_products as (

    select
        *
    from {{ ref('int_rxnorm_clinical_products_to_ingredient_components') }}
),

ingredient_component_crosswalk_codes as (

    select
        *
    from {{ ref('int_umls_ingredient_components_to_crosswalk_codes') }}
),

multiple_ingredient_crosswalk_codes as (

    select
        *
    from {{ ref('int_umls_ingredient_components_to_crosswalk_codes') }}
),

precise_ingredient_crosswalk_codes as (

    select
        *
    from {{ ref('int_umls_ingredient_components_to_crosswalk_codes') }}
)

select
    clinical_products.clinical_product_rxcui,
    clinical_products.clinical_product_name,
    clinical_products.clinical_product_tty,
    crosswalk_codes.*
from clinical_products
inner join ingredient_component_crosswalk_codes
    on ingredient_component_crosswalk_codes.ingredient_component_rxcui = clinical_products.ingredient_component_rxcui
