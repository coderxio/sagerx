-- stg_rxclass__has_epc.sql

with has_epc as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'has_epc'

)

select distinct
    *
from has_epc