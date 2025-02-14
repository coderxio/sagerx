-- stg_rxclass__ci_moa.sql

with ci_moa as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'ci_moa'
)

select distinct
    *
from ci_moa