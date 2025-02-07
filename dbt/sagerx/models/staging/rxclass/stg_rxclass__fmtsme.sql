-- stg_rxclass__fmtsme.sql

with fmtsme as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela_source = 'FMTSME'

)

select distinct
    *
from fmtsme