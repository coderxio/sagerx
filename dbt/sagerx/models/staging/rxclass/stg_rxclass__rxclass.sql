-- stg_rxclass__rxclass.sql

with rxclass as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}

)

select distinct
    *
from rxclass