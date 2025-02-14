-- stg_rxclass__atc.sql

with atc as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela_source = 'ATC'

)

select distinct
    *
from atc