-- stg_rxclass__medrt.sql

with medrt as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela_source = 'MEDRT'

)

select distinct
    *
from medrt