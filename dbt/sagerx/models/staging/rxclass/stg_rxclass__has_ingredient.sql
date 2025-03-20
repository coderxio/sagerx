-- stg_rxclass__has_ingredient.sql

with has_ingredient as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'has_ingredient'
)

select distinct
    *
from has_ingredient