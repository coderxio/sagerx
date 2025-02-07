-- stg_rxclass__ci_with.sql

with ci_with as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'ci_with'
)

select distinct
    *
from ci_with