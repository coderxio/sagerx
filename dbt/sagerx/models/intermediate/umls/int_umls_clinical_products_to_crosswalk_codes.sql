-- int_umls_clinical_products_to_crosswalk_codes.sql

with

clinical_products_to_ingredient_components as (

    select
        clinical_product_rxcui,
        clinical_product_name,
        clinical_product_tty,
        ingredient_component_rxcui,
        ingredient_component_name,
        ingredient_component_tty
    from {{ ref('int_rxnorm_clinical_products_to_ingredient_components') }}

),

clinical_products_to_multiple_ingredients as (

    select
        ingredient_rxcui as multiple_ingredient_rxcui,
        *
    from {{ ref('int_rxnorm_clinical_products_to_ingredients') }}
    where ingredient_tty = 'MIN'

),

clinical_products_to_precise_ingredients as (

    select
        *
    from {{ ref('int_rxnorm_clinical_products_to_ingredient_strengths') }}
    where precise_ingredient_rxcui is not null
),

ingredient_component_crosswalk_codes as (

    select
        *
    from {{ ref('int_umls_ingredient_components_to_crosswalk_codes') }}

),

multiple_ingredient_crosswalk_codes as (

    select
        *
    from {{ ref('int_umls_multiple_ingredients_to_crosswalk_codes') }}

),

precise_ingredient_crosswalk_codes as (

    select
        *
    from {{ ref('int_umls_precise_ingredients_to_crosswalk_codes') }}

),

final as (

    select
        clinical_products.clinical_product_rxcui,
        clinical_products.clinical_product_name,
        clinical_products.clinical_product_tty,
        crosswalk_codes.ingredient_component_rxcui as via_ingredient_rxcui,
        crosswalk_codes.ingredient_component_name as via_ingredient_name,
        crosswalk_codes.ingredient_component_tty as via_ingredient_tty,
        crosswalk_codes.rela,
        crosswalk_codes.class_id,
        crosswalk_codes.class_name,
        crosswalk_codes.class_type,
        crosswalk_codes.rela_source,
        crosswalk_codes.from_source,
        crosswalk_codes.from_code,
        crosswalk_codes.to_source,
        crosswalk_codes.to_code,
        crosswalk_codes.to_name
    from clinical_products_to_ingredient_components clinical_products
    inner join ingredient_component_crosswalk_codes crosswalk_codes
        on crosswalk_codes.ingredient_component_rxcui = clinical_products.ingredient_component_rxcui

    union

    select
        clinical_products.clinical_product_rxcui,
        clinical_products.clinical_product_name,
        clinical_products.clinical_product_tty,
        crosswalk_codes.multiple_ingredient_rxcui as via_ingredient_rxcui,
        crosswalk_codes.multiple_ingredient_name as via_ingredient_name,
        crosswalk_codes.multiple_ingredient_tty as via_ingredient_tty,
        crosswalk_codes.rela,
        crosswalk_codes.class_id,
        crosswalk_codes.class_name,
        crosswalk_codes.class_type,
        crosswalk_codes.rela_source,
        crosswalk_codes.from_source,
        crosswalk_codes.from_code,
        crosswalk_codes.to_source,
        crosswalk_codes.to_code,
        crosswalk_codes.to_name
    from clinical_products_to_multiple_ingredients clinical_products
    inner join multiple_ingredient_crosswalk_codes crosswalk_codes
        on crosswalk_codes.multiple_ingredient_rxcui = clinical_products.multiple_ingredient_rxcui

    union

    select
        clinical_products.clinical_product_rxcui,
        clinical_products.clinical_product_name,
        clinical_products.clinical_product_tty,
        crosswalk_codes.precise_ingredient_rxcui as via_ingredient_rxcui,
        crosswalk_codes.precise_ingredient_name as via_ingredient_name,
        crosswalk_codes.precise_ingredient_tty as via_ingredient_tty,
        crosswalk_codes.rela,
        crosswalk_codes.class_id,
        crosswalk_codes.class_name,
        crosswalk_codes.class_type,
        crosswalk_codes.rela_source,
        crosswalk_codes.from_source,
        crosswalk_codes.from_code,
        crosswalk_codes.to_source,
        crosswalk_codes.to_code,
        crosswalk_codes.to_name
    from clinical_products_to_precise_ingredients clinical_products
    inner join precise_ingredient_crosswalk_codes crosswalk_codes
        on crosswalk_codes.precise_ingredient_rxcui = clinical_products.precise_ingredient_rxcui

)

select * from final
