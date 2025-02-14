-- stg_rxclass__va.sql

with va as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela_source = 'VA'

)

select distinct
    *
from va