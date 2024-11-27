-- stg_rxclass__atc_codes_to_rxnorm_product_rxcuis.sql

with atc_codes_to_rxnorm_product_rxcuis as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela_source = 'ATCPROD'

)

select
    *
from atc_codes_to_rxnorm_product_rxcuis
