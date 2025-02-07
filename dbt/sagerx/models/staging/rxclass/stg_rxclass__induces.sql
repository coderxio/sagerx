-- stg_rxclass__induces.sql

with induces as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'induces'
)

select distinct
    *
from induces