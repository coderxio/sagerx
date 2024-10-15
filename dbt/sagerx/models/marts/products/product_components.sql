-- product_components.
-- primary key should be clinical_product_rxcui
-- + clinical_product_component_rxcui

with

product_components as (

    -- NOTE: this includes ingredients and dose forms
    select
        clinical_product_rxcui
        , clinical_product_name
        , clinical_product_tty
        , clinical_product_component_rxcui
        , clinical_product_component_name
        , clinical_product_component_tty
        , dose_form_rxcui
        , dose_form_name
        , dose_form_tty
        , ingredient_rxcui
        , ingredient_name
        , ingredient_tty
        , ingredient_component_rxcui
        , ingredient_component_name
        , ingredient_component_tty
        , active
        , prescribable    
    from {{ ref('int_rxnorm_clinical_products_to_ingredient_components') }}

)

select * from product_components
