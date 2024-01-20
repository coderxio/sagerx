with

rxterms as (

    select * from {{ source('rxterms', 'rxterms') }}
    
)

select distinct
    rxcui
    , display_name as name
    , concat(strength, ' ', new_dose_form) as strength
from rxterms
where suppress_for is null 
    and is_retired is null
