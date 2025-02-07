-- stg_rxclass__ci_chemclass.sql

with ci_chemclass as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'ci_chemclass'
)

select distinct
    *
from ci_chemclass