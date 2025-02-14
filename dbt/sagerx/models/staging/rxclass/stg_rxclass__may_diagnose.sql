-- stg_rxclass__may_diagnose.sql

with may_diagnose as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'may_diagnose'
)

select distinct
    *
from may_diagnose