-- stg_fda_unfinished__substances.sql

with

product as (
    
    select * from {{ source('fda_unfinished', 'fda_unfinished_product') }}

)

select distinct
    product.productid
    , row_number() over (partition by product.productid) as substance_line
    , arr.*
from product
    , unnest(string_to_array(product.substancename, '; ')
            ,string_to_array(product.active_numerator_strength, '; ')
            ,string_to_array(product.active_ingred_unit, '; ')
        ) arr(substancename, active_numerator_strength, active_ingred_unit)
