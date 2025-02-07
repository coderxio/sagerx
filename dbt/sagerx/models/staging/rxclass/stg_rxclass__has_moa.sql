-- stg_rxclass__has_moa.sql

with has_moa as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela in ('has_moa', 'isa_disposition')
)

select distinct
    *
from has_moa