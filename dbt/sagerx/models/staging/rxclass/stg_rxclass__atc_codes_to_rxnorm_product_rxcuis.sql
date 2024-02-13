-- stg_rxclass__atc_codes_to_rxnorm_product_rxcuis.sql

with atc_codes_to_rxnorm_product_rxcuis as (
    
    select * from {{ source('rxclass', 'rxclass_atc_to_product') }}

)

select
    index::varchar as rxcui
    , "classId"::varchar as class_id
    , "className"::varchar as class_name
    , "classType"::varchar as class_type
from atc_codes_to_rxnorm_product_rxcuis
