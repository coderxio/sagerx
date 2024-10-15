-- product_component_ingredients.sql

with

product_component_ingredients as (

    -- NOTE: this goes from product to product component to
    -- individual ingredients, precise ingredients, and
    -- ingredient strengths
    select * from {{ ref('int_rxnorm_clinical_products_to_ingredient_strengths' )}}

)

select * from product_component_ingredients
