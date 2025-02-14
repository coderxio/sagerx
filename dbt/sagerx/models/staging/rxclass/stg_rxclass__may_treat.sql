-- stg_rxclass__may_treat.sql

with may_treat as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'may_treat'
)

select distinct
    *
from may_treat