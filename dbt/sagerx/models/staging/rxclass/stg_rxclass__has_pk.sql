-- stg_rxclass__has_pk.sql

with has_pk as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'has_pk'
)

select distinct
    *
from has_pk