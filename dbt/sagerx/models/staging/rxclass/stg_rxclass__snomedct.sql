-- stg_rxclass__snomedct.sql

with snomedct as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela_source = 'SNOMEDCT'

)

select distinct
    *
from snomedct