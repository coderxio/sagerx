-- stg_rxclass__cdc.sql

with cdc as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela_source = 'CDC'

)

select distinct
    *
from cdc