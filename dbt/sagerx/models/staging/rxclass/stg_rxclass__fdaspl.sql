-- stg_rxclass__fdaspl.sql

with fdaspl as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela_source = 'FDASPL'

)

select distinct
    *
from fdaspl