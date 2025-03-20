-- stg_rxclass__has_active_metabolites.sql

with has_active_metabolites as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela = 'has_active_metabolites'
)

select distinct
    *
from has_active_metabolites