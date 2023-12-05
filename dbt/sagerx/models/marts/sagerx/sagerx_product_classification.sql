with atc_to_product as (
    select * from {{ ref('int_atc_codes_to_product_rxcuis') }}
),

rxnorm_product as (
    select * from {{ ref('int_rxnorm_clinical_products_to_ingredients') }}
)

select
    --atc_1_name
    atc_2_name
    , atc_3_name
    , atc_4_name
    --, atc_5_name
    , rxnorm_product.ingredient_name
    , concat(rxnorm_product.ingredient_name, ' TBD ') as precise_ingredient_name
    , concat(rxnorm_product.ingredient_name, ' TBD ', dose_form_name) as dose_form_name
    , concat(rxnorm_product.ingredient_name, ' TBD ', dose_form_name, ' TBD') as strength_name
    --, rxnorm_product.*
from atc_to_product
left join rxnorm_product
    on rxnorm_product.clinical_product_rxcui = atc_to_product.rxcui::varchar
where rxnorm_product.clinical_product_rxcui is not null
