-- stg_rxclass__rxnorm.sql

with rxnorm as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela_source = 'RXNORM'

)

select distinct
    *
from rxnorm