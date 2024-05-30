-- products_to_inactive_ingredients.sql

with products_to_inactive_ingredients as (
    select * from {{ ref('int_mthspl_products_to_inactive_ingredients') }}
)

select * from products_to_inactive_ingredients
