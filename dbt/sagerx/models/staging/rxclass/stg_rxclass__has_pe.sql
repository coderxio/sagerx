-- stg_rxclass__has_pe.sql

with has_pe as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'has_pe'
)

select distinct
    *
from has_pe