-- stg_rxclass__atcprod.sql

with atcprod as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela_source = 'ATCPROD'

)

select
    *
from atcprod
