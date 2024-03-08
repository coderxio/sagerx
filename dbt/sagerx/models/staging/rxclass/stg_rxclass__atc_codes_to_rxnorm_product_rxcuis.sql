-- stg_rxclass__atc_codes_to_rxnorm_product_rxcuis.sql

with atc_codes_to_rxnorm_product_rxcuis as (
    
    select * from {{ source('rxclass', 'rxclass_atc_to_product') }}

)

select
    *
from atc_codes_to_rxnorm_product_rxcuis
