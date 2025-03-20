-- stg_rxclass__isa_therapeutic.sql

with isa_therapeutic as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'isa_therapeutic'
)

select distinct
    *
from isa_therapeutic