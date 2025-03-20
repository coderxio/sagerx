-- stg_rxclass__dailymed.sql

with dailymed as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela_source = 'DAILYMED'

)

select distinct
    *
from dailymed