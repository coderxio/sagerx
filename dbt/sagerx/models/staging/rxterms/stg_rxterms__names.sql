with

rxterms as (

    select * from {{ source('rxterms', 'rxterms') }}
    
)

select distinct
    display_name as name
    , display_name_synonym as synonyms
from rxterms
where suppress_for is null 
    and is_retired is null
