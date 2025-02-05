-- stg_rxclass__has_tc.sql

with has_tc as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'has_tc'
)

select distinct
    *
from has_tc