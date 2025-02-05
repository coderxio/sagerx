-- stg_rxclass__has_chemical_structure.sql

with has_chemical_structure as (
    
    select
        *
    from {{ source('rxclass', 'rxclass') }}
    where rela in ('isa_structure', 'has_chemical_structure')
)

select distinct
    *
from has_chemical_structure