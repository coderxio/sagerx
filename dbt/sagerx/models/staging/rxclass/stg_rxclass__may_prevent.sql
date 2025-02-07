-- stg_rxclass__may_prevent.sql

with may_prevent as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'may_prevent'
)

select distinct
    *
from may_prevent